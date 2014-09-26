""" Main module containing the API """

import msgpack
import socket
import ipaddress
import threading
import time

from .bucketset import BucketSet
from .hashing   import hash_function, random_id, id_bytes
from .peer      import Peer
from .shortlist import Shortlist
from .helper    import sixunicode
from .server    import DHTServer, DHTRequestHandler

k = 20
alpha = 3
id_bits = id_bytes * 8
iteration_sleep = 1

Shortlist.iteration_sleep = iteration_sleep

# TODO: Maintainance thread / rpc_list cleanup

class DHT(object):

    class NetworkError(Exception):
        pass

    def __init__(
            self,
            port,
            hostv4=None,
            hostv6=None,
            id_=None,
            boot_host=None,
            boot_port=None
    ):
        if not id_:
            id_ = random_id()
        self.peer = Peer(port, id_, hostv4, hostv6)
        self.data = {}
        self.buckets = BucketSet(k, id_bits, self.peer.id)
        self.rpc_ids = {}  # should probably have a lock for this
        self.server4 = None
        self.server6 = None
        if hostv4:
            self.server4 = DHTServer(
                self.peer.addressv4(),
                DHTRequestHandler,
                is_v6=False
            )
            self.server4.dht = self
            self.server4_thread = threading.Thread(
                target=self.server4.serve_forever
            )
            self.server4_thread.daemon = True
            self.server4_thread.start()
        if hostv6:
            self.server6 = DHTServer(
                self.peer.addressv6(),
                DHTRequestHandler,
                is_v6=True
            )
            self.server6.dht = self
            self.server6_thread = threading.Thread(
                target=self.server6.serve_forever
            )
            self.server6_thread.daemon = True
            self.server6_thread.start()
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
                    peer.find_value(key, rpc_id, dht=self, peer_id=self.peer.id)
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

    def bootstrap(self, boot_host, boot_port):
        addr = socket.getaddrinfo(boot_host, boot_port)[0][4][0]
        ipaddr = ipaddress.ip_address(sixunicode(addr))
        if isinstance(ipaddr, ipaddress.IPv6Address):
            boot_peer = Peer(boot_port, 0, hostv6=str(ipaddr))
        else:
            boot_peer = Peer(boot_port, 0, hostv4=str(ipaddr))
        self.iterative_find_nodes(random_id(), boot_peer=boot_peer)
        tries = 0
        while len(self.buckets.nearest_nodes(self.peer.id)) < 1:
            tries += 1
            if tries > 2:
                raise DHT.NetworkError("Cannot boot DHT")
            time.sleep(3)
            self.iterative_find_nodes(random_id(), boot_peer=boot_peer)

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
