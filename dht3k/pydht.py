""" Main module containing the API """

import msgpack
import socket
import ipaddress
import threading
import time
import contextlib

from .bucketset import BucketSet
from .hashing   import hash_function, random_id, id_bytes
from .peer      import Peer
from .shortlist import Shortlist
from .helper    import sixunicode
from .server    import DHTServer, DHTRequestHandler
from .const     import Message

k = 20
alpha = 3
id_bits = id_bytes * 8
iteration_sleep = 1

Shortlist.iteration_sleep = iteration_sleep

# TODO: Maintainance thread / rpc_list cleanup


def has_dual_stack(sock=None):
    """Return True if kernel allows creating a socket which is able to
    listen for both IPv4 and IPv6 connections.
    If *sock* is provided the check is made against it.
    """
    try:
        socket.AF_INET6  # noqa
        socket.IPPROTO_IPV6  # noqa
        socket.IPV6_V6ONLY  # noqa
    except AttributeError:
        return False
    try:
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
            return True
        else:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            with contextlib.closing(sock):
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
                return True
    except socket.error:
        return False


class DHT(object):

    class NetworkError(Exception):
        pass

    def __init__(
            self,
            port,
            hostv4        = None,
            hostv6        = None,
            id_           = None,
            boot_host     = None,
            boot_port     = None,
            listen_hostv4 = "",
            listen_hostv6 = "",
    ):
        if not id_:
            id_ = random_id()
        self.peer = Peer(port, id_, hostv4, hostv6)
        self.data = {}
        self.buckets = BucketSet(k, id_bits, self.peer.id)
        self.rpc_ids = {}  # should probably have a lock for this
        self.server4 = None
        self.server6 = None
        if not hostv4:
            self.hostv4 = None
        else:
            self.hostv4  = ipaddress.ip_address(hostv4)
        if not hostv6:
            self.hostv6 = None
        else:
            self.hostv6  = ipaddress.ip_address(hostv6)
        self.dual_stack = False
        if hostv6 is not None or self.dual_stack:
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
            self.dual_stack = has_dual_stack(self.server6.socket)
        if hostv4 is not None and not self.dual_stack:
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
        if boot_host:
            self.bootstrap(boot_host, boot_port)

    def close(self):
        if self.server4:
            self.server4.shutdown()
            self.server4.server_close()
        if self.server6:
            self.server6.shutdown()
            self.server6.server_close()

    def iterative_find_nodes(self, key, boot_peer=None):
        shortlist = Shortlist(k, key, self.peer.id)
        shortlist.update(self.buckets.nearest_nodes(key))
        if boot_peer:
            rpc_id = random_id()
            self.rpc_ids[rpc_id] = shortlist
            shortlist.updated.clear()
            boot_peer.find_node(key, rpc_id, dht=self, peer_id=self.peer.id)
            shortlist.updated.wait(iteration_sleep)
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(alpha)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id = random_id()
                    self.rpc_ids[rpc_id] = shortlist
                    shortlist.updated.clear()
                    peer.find_node(key, rpc_id, dht=self, peer_id=self.peer.id)
                    shortlist.updated.wait(iteration_sleep)
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
        shortlist = Shortlist(k, key, self.peer.id)
        shortlist.update(self.buckets.nearest_nodes(key))
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(alpha)
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
                    shortlist.updated.wait(iteration_sleep)
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

    def stun_warning(self, found, defined):
        """ Log a warning about wrong public address """
        # TODO: To logging
        print(
"Warning: defined public address (%s) does not match the\n"  # noqa
"address found by the bootstap peer (%s). We will use the\n"  # noqa
"defined address. IPv4/6 convergence will not be optimal!" % (  # noqa
    defined, found
)
        )

    def stun_result(self, res):
        """ Set the stun result in the client """
        for me_msg in res[1:]:
            try:
                me_tuple = me_msg[Message.STUN_ADDR]
                me_peer = Peer(*me_tuple)
                if me_peer.hostv4:
                    if not self.hostv4:
                        self.peer.hostv4 = me_peer.hostv4
                    elif me_peer.hostv4 != self.hostv4:
                        self.stun_warning(me_peer.hostv4, self.hostv4)
                if me_peer.hostv6:
                    if not self.hostv6:
                        self.peer.hostv6 = me_peer.hostv6
                    elif me_peer.hostv6 != self.hostv6:
                        self.stun_warning(me_peer.hostv6, self.hostv6)
            except TypeError:
                pass

    def bootstrap(self, boot_host, boot_port):
        addr = socket.getaddrinfo(boot_host, boot_port)[0][4][0]
        ipaddr = ipaddress.ip_address(sixunicode(addr))
        if isinstance(ipaddr, ipaddress.IPv6Address):
            boot_peer = Peer(boot_port, 0, hostv6=str(ipaddr))
        else:
            boot_peer = Peer(boot_port, 0, hostv4=str(ipaddr))

        rpc_id = random_id()
        self.rpc_ids[rpc_id] = [time.time()]
        boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
        time.sleep(1)

        peer_found = False
        if len(self.rpc_ids[rpc_id]) > 1:
            try:
                message = self.rpc_ids[rpc_id][1]
                boot_peer = Peer(*message[Message.ALL_ADDR])
                peer_found = True
            except KeyError:
                self.rpc_ids[rpc_id].pop(1)
        if not peer_found:
            time.sleep(3)
            boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
            if len(self.rpc_ids[rpc_id]) > 1:
                self.stun_result(self.rpc_ids[rpc_id])
            else:
                raise DHT.NetworkError("Cannot boot DHT")
        del self.rpc_ids[rpc_id]

        rpc_id = random_id()
        self.rpc_ids[rpc_id] = [time.time()]
        boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
        time.sleep(1)

        if len(self.rpc_ids[rpc_id]) > 2:
            self.stun_result(self.rpc_ids[rpc_id])
        else:
            time.sleep(3)
            boot_peer.ping(self, self.peer.id, rpc_id = rpc_id)
            if len(self.rpc_ids[rpc_id]) > 1:
                self.stun_result(self.rpc_ids[rpc_id])
            else:
                raise DHT.NetworkError("Cannot boot DHT")
        del self.rpc_ids[rpc_id]

        self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
        if len(self.buckets.nearest_nodes(self.peer.id)) < 1:
            time.sleep(3)
            self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
            if len(self.buckets.nearest_nodes(self.peer.id)) < 1:
                raise DHT.NetworkError("Cannot boot DHT")

    def __getitem__(self, key):
        hashed_key = hash_function(msgpack.dumps(key))
        if hashed_key in self.data:
            return self.data[hashed_key]
        return self.iterative_find_value(hashed_key)

    def __setitem__(self, key, value):
        hashed_key = hash_function(msgpack.dumps(key))
        nearest_nodes = self.iterative_find_nodes(hashed_key)
        self.data[hashed_key] = value
        for node in nearest_nodes:
            node.store(hashed_key, value, dht=self, peer_id=self.peer.id)
