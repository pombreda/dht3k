""" Main module containing the API """

import msgpack
import socket
import ipaddress
import threading
import time

from .bucketset import BucketSet
from .hashing   import hash_function, random_id
from .peer      import Peer
from .shortlist import Shortlist
from .helper    import sixunicode
from .server    import DHTServer, DHTRequestHandler
from .const     import Message, Config


# TODO: Maintainance thread / rpc_list cleanup

__all__ = ['DHT']


class DHT(object):

    class NetworkError(Exception):
        pass

    def __init__(
            self,
            port             = 7339,
            hostv4           = None,
            hostv6           = None,
            id_              = None,
            boot_host        = None,
            boot_port        = None,
            listen_hostv4    = "",
            listen_hostv6    = "",
            zero_config      = False,
            default_encoding = None
    ):
        if not id_:
            id_ = random_id()
        self.encoding = default_encoding
        self.peer = Peer(port, id_, hostv4, hostv6)
        self.data = {}
        self.buckets = BucketSet(Config.K, Config.ID_BITS, self.peer.id)
        self.rpc_ids = {}  # should probably have a lock for this
        self.server4 = None
        self.server6 = None
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
        if zero_config:
            try:
                self._bootstrap("54.164.229.197", 7339)
            except DHT.NetworkError:
                self._bootstrap("2001:470:7:ab::2", 7339)
        else:
            if boot_host:
                self._bootstrap(boot_host, boot_port)

    def close(self):
        if self.server4:
            self.server4.shutdown()
            self.server4.server_close()
        if self.server6:
            self.server6.shutdown()
            self.server6.server_close()

    def iterative_find_nodes(self, key, boot_peer=None):
        shortlist = Shortlist(Config.K, key, self.peer.id)
        shortlist.update(self.buckets.nearest_nodes(key))
        if boot_peer:
            rpc_id = random_id()
            self.rpc_ids[rpc_id] = shortlist
            shortlist.updated.clear()
            boot_peer.find_node(key, rpc_id, dht=self, peer_id=self.peer.id)
            shortlist.updated.wait(Config.SLEEP_WAIT)
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(Config.ALPHA)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id = random_id()
                    self.rpc_ids[rpc_id] = shortlist
                    shortlist.updated.clear()
                    peer.find_node(key, rpc_id, dht=self, peer_id=self.peer.id)
                    shortlist.updated.wait(Config.SLEEP_WAIT)
            return shortlist.results()
        finally:
            end = time.time()
            # Convert to logging
            print("find_nodes: %.5fs (%d, %d, %d)" % (
                (end - start),
                len(shortlist.list),
                len([it for it in shortlist.list if it[1]]),
                len(list(self.buckets.peers())),
            ))

    def iterative_find_value(self, key):
        shortlist = Shortlist(Config.K, key, self.peer.id)
        shortlist.update(self.buckets.nearest_nodes(key))
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(Config.ALPHA)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id = random_id()
                    self.rpc_ids[rpc_id] = shortlist
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
            print("find_value: %.5fs (%d, %d, %d)" % (
                (end - start),
                len(shortlist.list),
                len([it for it in shortlist.list if it[1]]),
                len(list(self.buckets.peers())),
            ))

    def _discov_warning(self, found, defined):
        """ Log a warning about wrong public address """
        # TODO: To logging
        print(
"Warning: defined public address (%s) does not match the\n"  # noqa
"address found by the bootstap peer (%s). We will use the\n"  # noqa
"defined address. IPv4/6 convergence will not be optimal!" % (  # noqa
    defined, found
)
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

    def _bootstrap(self, boot_host, boot_port):
        addr = socket.getaddrinfo(boot_host, boot_port)[0][4][0]
        ipaddr = ipaddress.ip_address(sixunicode(addr))
        if isinstance(ipaddr, ipaddress.IPv6Address):
            boot_peer = Peer(boot_port, 0, hostv6=str(ipaddr))
        else:
            boot_peer = Peer(boot_port, 0, hostv4=str(ipaddr))

        rpc_id = random_id()
        self.rpc_ids[rpc_id] = [time.time()]
        boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
        time.sleep(Config.SLEEP_WAIT)

        peer_found = False
        if len(self.rpc_ids[rpc_id]) > 1:
            try:
                message = self.rpc_ids[rpc_id][1]
                boot_peer = Peer(*message[Message.ALL_ADDR], is_bytes=True)
                peer_found = True
            except KeyError:
                self.rpc_ids[rpc_id].pop(1)
        if not peer_found:
            time.sleep(Config.SLEEP_WAIT * 3)
            boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
            if len(self.rpc_ids[rpc_id]) > 1:
                self._discov_result(self.rpc_ids[rpc_id])
            else:
                raise DHT.NetworkError("Cannot boot DHT")
        del self.rpc_ids[rpc_id]

        rpc_id = random_id()
        self.rpc_ids[rpc_id] = [time.time()]
        boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
        time.sleep(Config.SLEEP_WAIT)

        if len(self.rpc_ids[rpc_id]) > 2:
            self._discov_result(self.rpc_ids[rpc_id])
        else:
            time.sleep(Config.SLEEP_WAIT * 3)
            boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
            if len(self.rpc_ids[rpc_id]) > 1:
                self._discov_result(self.rpc_ids[rpc_id])
            else:
                raise DHT.NetworkError("Cannot boot DHT")
        del self.rpc_ids[rpc_id]

        self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
        if len(self.buckets.nearest_nodes(self.peer.id)) < 1:
            time.sleep(Config.SLEEP_WAIT * 3)
            self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
            if len(self.buckets.nearest_nodes(self.peer.id)) < 1:
                raise DHT.NetworkError("Cannot boot DHT")

    def get(self, key, encoding=None):
        if not encoding:
            encoding = self.encoding
        hashed_key = hash_function(msgpack.dumps(key))
        if hashed_key in self.data:
            res = self.data[hashed_key]
        else:
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
        self.data[hashed_key] = value
        for node in nearest_nodes:
            node.store(hashed_key, value, dht=self, peer_id=self.peer.id)

    def __setitem__(self, key, value):
        self.set(key, value)
