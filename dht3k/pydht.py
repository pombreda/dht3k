""" Main module containing the API """

import msgpack
import socket
import ipaddress
import threading
import time
import lazymq
import asyncio

from .bucketset import BucketSet
from .hashing   import hash_function, rpc_id_pair, random_id
from .peer      import Peer
from .shortlist import Shortlist
from .helper    import LockedDict
from .server    import DHTServer, DHTRequestHandler
from .const     import Message, Config, Storage
from .          import upnp
from .          import excepions
from .          import threads
from .log       import log_to_stderr, l


# TODO: set well_connected somewhere
# TODO: all threads must be gone
# TODO: idea storage limit per peer
# TODO: local peer id to storage (sqlite)
# TODO: no republish (write docu why)
# TODO: multi-value (per client GC)
# TODO: get with value-limit
# TODO: tcp (lazymq)
# TODO: storage limit
# TODO: check if we have a RPC state leak
# TODO: keep the same id (if port/IPs are the same??)
# TODO: data to disk (optional)
# TODO: async interface (futures)
# TODO: more/better unittest + 100% coverage
# TODO: what about IP changes?
# 1. Is there a binding problem?
# 2. Refactor address discover and make maint-thread
# TODO: Consider sending multiple answers (split!) if EMSGSIZE occurs
# TODO: pep8
# TODO: lint
# TODO: documentaion
# TODO: review
# TODO: think a bit more about security

__all__ = ['DHT']


class DHT(object):

    _log_enabled = False
    MaxSizeException = excepions.MaxSizeException
    NetworkError     = excepions.NetworkError

    def __init__(
            self,
            port             = Config.PORT,
            host_v6          = None,
            host_v4          = None,
            id_              = None,
            bind_v6          = "",
            bind_v4          = "",
            default_encoding = None,
            port_map         = True,
            network_id       = Config.NETWORK_ID,
            storage          = Storage.MEMORY,
            cert_chain_pem   = None,
            log              = True,
            debug            = True,
            loop             = None,
    ):
        self._loop = loop
        if log:
            log_to_stderr(debug)
        if not id_:
            id_ = random_id()
        if port < 1024:
            raise DHT.NetworkError("Ports below 1024 are not allowed")
        self.stop       = threading.Event()
        self.encoding   = default_encoding
        self.peer       = Peer(port, id_, host_v4, host_v6)
        self.port       = port
        self.host_v6    = host_v6
        self.host_v4    = host_v4
        self.firewalled = None

        if storage == Storage.NONE:
            self.data = None
        else:
            self.data = LockedDict()
        self.buckets = BucketSet(Config.K, Config.ID_BITS, self.peer.id)
        self.rpc_states = LockedDict()
        self.boot_peer = None
        self.network_id = network_id
        self.lazymq        = lazymq.LazyMQ(
            port           = self.port,
            encoding       = self.encoding,
            bind_v6        = bind_v6,
            bind_v4        = bind_v4,
            cert_chain_pem = cert_chain_pem,
            loop           = self._loop,
        )
        # TODO: move this to a thread
        if port_map:
            if not upnp.try_map_port(port):
                l.warning("UPnP could not map port")

    @asyncio.coroutine
    def start(
            self,
            boot_host        = None,
            boot_port        = None,
            zero_config      = False,
    ):
        self._zero_config = zero_config
        if not self.host_v6:
            if zero_config:
                self.host_v6 = ""
        else:
            self.host_v6   = ipaddress.ip_address(self.host_v6)
        if not self.host_v4:
            if zero_config:
                self.host_v4 = ""
        else:
            self.host_v4   = ipaddress.ip_address(self.host_v4)
        if boot_host or zero_config:
            self.firewalled = True
        else:
            self.firewalled = False
        if self._zero_config:
            try:
                self._bootstrap("31.171.244.153", Config.PORT)
            except DHT.NetworkError:
                self._bootstrap("2001:470:7:ab::2", Config.PORT)
        else:
            if boot_host:
                self._bootstrap(boot_host, boot_port)
        # TODO: this needs to be coros
        self.bucket_refrsh  = threads.run_bucket_refresh(self)
        self.check_firewall = threads.run_check_firewalled(self)
        self.rpc_cleanup    = threads.run_rpc_cleanup(self)


    @asyncio.coroutine
    def close(self):
        self.stop.set()
        self.bucket_refrsh.join()
        self.check_firewall.join()
        self.rpc_cleanup.join()
        yield from self.lazymq.close()

    def iterative_find_nodes(self, key, boot_peer=None):
        shortlist = Shortlist(Config.K, key, self.peer.id)
        shortlist.update(self.buckets.nearest_nodes(key))
        if boot_peer:
            rpc_id, hash_id = rpc_id_pair()
            with self.rpc_states as states:
                states[hash_id] = [time.time(), shortlist]
            shortlist.updated.clear()
            boot_peer.find_node(key, rpc_id, dht=self, peer_id=self.peer.id)
            shortlist.updated.wait(Config.SLEEP_WAIT)
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(Config.ALPHA)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id, hash_id = rpc_id_pair()
                    with self.rpc_states as states:
                        states[hash_id] = [time.time(), shortlist]
                    shortlist.updated.clear()
                    peer.find_node(key, rpc_id, dht=self, peer_id=self.peer.id)
                shortlist.updated.wait(Config.SLEEP_WAIT)
            return shortlist.results()
        finally:
            end = time.time()
            # Convert to logging
            l.info(
                "find_nodes: %.5fs (%d, %d, %d)",
                (end - start),
                len(shortlist.list),
                len([it for it in shortlist.list if it[1]]),
                len(self.buckets.peerslist()),
            )

    def iterative_find_value(self, key):
        shortlist = Shortlist(Config.K, key, self.peer.id)
        shortlist.update(self.buckets.nearest_nodes(key))
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(Config.ALPHA)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id, hash_id = rpc_id_pair()
                    with self.rpc_states as states:
                        states[hash_id] = [time.time(), shortlist]
                    shortlist.updated.clear()
                    peer.find_value(
                        key,
                        rpc_id,
                        dht=self,
                        peer_id=self.peer.id
                    )
                shortlist.updated.wait(Config.SLEEP_WAIT)
                if shortlist.completion_value.done():
                    return shortlist.completion_result()
            return shortlist.completion_result()
        finally:
            end = time.time()
            # Convert to logging
            l.info(
                "find_value: %.5fs (%d, %d, %d)",
                (end - start),
                len(shortlist.list),
                len([it for it in shortlist.list if it[1]]),
                len(self.buckets.peerslist()),
            )

    def _discov_warning(self, found, defined):
        """ Log a warning about wrong public address """
        # TODO: To logging
        l.warn(
            """Warning: defined public address (%s) does not match the
            address found by the bootstap peer (%s). We will use the
            defined address. IPv4/6 convergence will not be optimal!""",
            defined,
            found
        )

    def _discov_result(self, res):
        """ Set the discover result in the client """
        for me_msg in res[1:]:
            try:
                me_tuple = me_msg[Message.CLI_ADDR]
                me_peer = Peer(*me_tuple)
                if me_peer.host_v4:
                    if not self.host_v4:
                        self.peer.host_v4 = me_peer.host_v4
                    elif me_peer.host_v4 != self.host_v4:
                        self._discov_warning(me_peer.host_v4, self.host_v4)
                if me_peer.host_v6:
                    if not self.host_v6:
                        self.peer.host_v6 = me_peer.host_v6
                    elif me_peer.host_v6 != self.host_v6:
                        self._discov_warning(me_peer.host_v6, self.host_v6)
            except TypeError:
                pass

    def _len_states(self, hash_id):
        """ Return length of rpc states """
        with self.rpc_states as states:
            return len(states[hash_id])

    def _bootstrap(self, boot_host, boot_port):
        addr = socket.getaddrinfo(boot_host, boot_port)[0][4][0]
        ipaddr = ipaddress.ip_address(addr)
        if isinstance(ipaddr, ipaddress.IPv6Address):
            boot_peer = Peer(boot_port, 0, host_v6=str(ipaddr))
        else:
            boot_peer = Peer(boot_port, 0, host_v4=str(ipaddr))

        rpc_id, hash_id = rpc_id_pair()
        with self.rpc_states as states:
            states[hash_id] = [time.time()]
        boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
        time.sleep(Config.SLEEP_WAIT)

        peer_found = False

        if self._len_states(hash_id) > 1:
            try:
                with self.rpc_states as states:
                    message = states[hash_id][1]
                boot_peer = Peer(*message[Message.ALL_ADDR])
                peer_found = True
            except KeyError:
                with self.rpc_states as states:
                    states[hash_id].pop(1)
        if not peer_found:
            time.sleep(Config.SLEEP_WAIT * 3)
            boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
            time.sleep(Config.SLEEP_WAIT)
            if self._len_states(hash_id) > 1:
                with self.rpc_states as states:
                    self._discov_result(states[hash_id])
            else:
                raise DHT.NetworkError("Cannot boot DHT")
        with self.rpc_states as states:
            del states[hash_id]

        rpc_id, hash_id = rpc_id_pair()

        with self.rpc_states as states:
            states[hash_id] = [time.time()]
        boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
        time.sleep(Config.SLEEP_WAIT)

        if self._len_states(hash_id) > 2:
            with self.rpc_states as states:
                self._discov_result(states[hash_id])
        else:
            time.sleep(Config.SLEEP_WAIT * 3)
            boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
            time.sleep(Config.SLEEP_WAIT)
            if self._len_states(hash_id) > 1:
                with self.rpc_states as states:
                    self._discov_result(states[hash_id])
            else:
                raise DHT.NetworkError("Cannot boot DHT")
        with self.rpc_states as states:
            del states[hash_id]

        self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
        if len(self.buckets.nearest_nodes(self.peer.id)) < 1:
            time.sleep(Config.SLEEP_WAIT * 3)
            self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
            time.sleep(Config.SLEEP_WAIT)
            if len(self.buckets.nearest_nodes(self.peer.id)) < 1:
                raise DHT.NetworkError("Cannot boot DHT")
        self.boot_peer = boot_peer
        l.info("DHT is bootstrapped")

    def get(self, key, encoding=None):
        if not encoding:
            encoding = self.encoding
        hashed_key = hash_function(msgpack.dumps(key))
        res = None
        if self.data:
            with self.data as data:
                if hashed_key in data:
                    res = data[hashed_key]
        if res is None:
            res = self.iterative_find_value(hashed_key)
        if encoding:
            res = msgpack.loads(res, encoding=encoding)
        return res

    def __getitem__(self, key):
        return self.get(key)

    def set(self, key, value, encoding=None):
        if not encoding:
            encoding = self.encoding
        if encoding:
            value = msgpack.dumps(value, encoding=encoding)
        hashed_key = hash_function(msgpack.dumps(key))
        nearest_nodes = self.iterative_find_nodes(hashed_key)
        if self.data:
            with self.data as data:
                data[hashed_key] = value
        for node in nearest_nodes:
            node.store(hashed_key, value, dht=self, peer_id=self.peer.id)

    def __setitem__(self, key, value):
        self.set(key, value)
