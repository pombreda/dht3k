""" Main module containing the API """

import msgpack
import socket
import ipaddress
import threading
import time

from .bucketset import BucketSet
from .hashing   import hash_function, rpc_id_pair, random_id
from .peer      import Peer
from .shortlist import Shortlist
from .helper    import sixunicode, LockedDict
from .server    import DHTServer, DHTRequestHandler
from .const     import Message, Config, Storage
from .          import upnp
from .          import excepions
from .          import threads
from .log       import log_to_stderr, l


# TODO: check if we have a RPC state leak
# TODO: keep the same id (if port/IPs are the same??)
# TODO: data to disk (optional)
# TODO: async interface (futures)
# TODO: more/better unittest + 100% coverage
# TODO: what about IP changes?
# 1. Is there a binding problem?
# 2. Refactor address discover and make maint-thread
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
            hostv4           = None,
            hostv6           = None,
            id_              = None,
            boot_host        = None,
            boot_port        = None,
            listen_hostv4    = "",
            listen_hostv6    = "",
            zero_config      = False,
            default_encoding = None,
            port_map         = True,
            network_id       = Config.NETWORK_ID,
            storage          = Storage.MEMORY,
            log              = True,
            debug            = True,
    ):
        if log and not DHT._log_enabled:
            log_to_stderr(debug)
            DHT._log_enabled = True
        if not id_:
            id_ = random_id()
        if port < 1024:
            raise DHT.NetworkError("Ports below 1024 are not allowed")
        if boot_host or zero_config:
            self.firewalled = True
        else:
            self.firewalled = False
        self.stop = threading.Event()
        self.encoding = default_encoding
        self.peer = Peer(port, id_, hostv4, hostv6)
        if storage == Storage.NONE:
            self.data = None
        else:
            self.data = LockedDict()
        self.buckets = BucketSet(Config.K, Config.ID_BITS, self.peer.id)
        self.rpc_states = LockedDict()
        self.server4 = None
        self.server6 = None
        self.boot_peer = None
        self.network_id = network_id
        if not hostv4:
            if zero_config:
                hostv4 = ""
            self.hostv4 = hostv4
        else:
            self.hostv4  = ipaddress.ip_address(hostv4)
        if not hostv6:
            if zero_config:
                hostv6 = ""
            self.hostv6 = hostv6
        else:
            self.hostv6  = ipaddress.ip_address(hostv6)
        # Detecting dual_stack sockets seems not to work on some OSs
        # so we always use two sockets
        if hostv6 is not None:
            self.server6 = DHTServer(
                (listen_hostv6, port),
                DHTRequestHandler,
                is_v6=True
            )
            self.server6.dht = self
            self.server6_thread = threading.Thread(
                target=self.server6.serve_forever
            )
            self.server6_thread.daemon = True
            self.server6_thread.start()
            self.fw_sock6 = socket.socket(
                socket.AF_INET6,
                socket.SOCK_DGRAM
            )
            self.fw_sock6.setsockopt(
                socket.IPPROTO_IPV6,
                socket.IPV6_V6ONLY,
                True,
            )
            self.fw_sock6.bind((listen_hostv6, port + 1))
        if hostv4 is not None:
            self.server4 = DHTServer(
                (listen_hostv4, port),
                DHTRequestHandler,
                is_v6=False
            )
            self.server4.dht = self
            self.server4_thread = threading.Thread(
                target=self.server4.serve_forever
            )
            self.server4_thread.daemon = True
            self.server4_thread.start()
            self.fw_sock4 = socket.socket(
                socket.AF_INET,
                socket.SOCK_DGRAM
            )
            self.fw_sock4.bind((listen_hostv4, port + 1))
        if port_map:
            if not upnp.try_map_port(port):
                l.warning("UPnP could not map port")
        if zero_config:
            try:
                self._bootstrap("31.171.244.153", Config.PORT)
            except DHT.NetworkError:
                self._bootstrap("2001:470:7:ab::2", Config.PORT)
        else:
            if boot_host:
                self._bootstrap(boot_host, boot_port)
        self.bucket_refrsh  = threads.run_bucket_refresh(self)
        self.check_firewall = threads.run_check_firewalled(self)
        self.rpc_cleanup    = threads.run_rpc_cleanup(self)

    def close(self):
        self.stop.set()
        self.bucket_refrsh.join()
        self.check_firewall.join()
        self.rpc_cleanup.join()
        if self.server4:
            self.server4.shutdown()
            self.server4.server_close()
        if self.server6:
            self.server6.shutdown()
            self.server6.server_close()
        self.server4.idle.wait()
        self.server6.idle.wait()
        if self.server4:
            self.fw_sock4.close()
        if self.server6:
            self.fw_sock6.close()

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
        l.warn(  # noqa
"Warning: defined public address (%s) does not match the\n"  # noqa
"address found by the bootstap peer (%s). We will use the\n"  # noqa
"defined address. IPv4/6 convergence will not be optimal!",  # noqa
defined,  # noqa
found     # noqa
        )

    def _discov_result(self, res):
        """ Set the discover result in the client """
        for me_msg in res[1:]:
            try:
                me_tuple = me_msg[Message.CLI_ADDR]
                me_peer = Peer(*me_tuple, is_bytes=True)
                if me_peer.hostv4:
                    if not self.hostv4:
                        self.peer.hostv4 = me_peer.hostv4
                    elif me_peer.hostv4 != self.hostv4:
                        self._discov_warning(me_peer.hostv4, self.hostv4)
                if me_peer.hostv6:
                    if not self.hostv6:
                        self.peer.hostv6 = me_peer.hostv6
                    elif me_peer.hostv6 != self.hostv6:
                        self._discov_warning(me_peer.hostv6, self.hostv6)
            except TypeError:
                pass

    def _len_states(self, hash_id):
        """ Return length of rpc states """
        with self.rpc_states as states:
            return len(states[hash_id])

    def _bootstrap(self, boot_host, boot_port):
        addr = socket.getaddrinfo(boot_host, boot_port)[0][4][0]
        ipaddr = ipaddress.ip_address(sixunicode(addr))
        if isinstance(ipaddr, ipaddress.IPv6Address):
            boot_peer = Peer(boot_port, 0, hostv6=str(ipaddr))
        else:
            boot_peer = Peer(boot_port, 0, hostv4=str(ipaddr))

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
                boot_peer = Peer(*message[Message.ALL_ADDR], is_bytes=True)
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
