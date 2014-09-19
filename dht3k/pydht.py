import msgpack
import random
import socket
import concurrent.futures as futures
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
import threading
import time

from .bucketset import BucketSet
from .hashing   import hash_function, random_id, id_bytes
from .peer      import Peer
from .shortlist import Shortlist
from .const     import Message

k = 20
alpha = 3
id_bits = id_bytes * 8
iteration_sleep = 1


class DHTRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        try:
            message = msgpack.loads(self.request[0].strip())
            message_type = message[Message.MESSAGE_TYPE]
            if message_type == Message.PING:
                self.handle_ping(message)
            elif message_type == Message.PONG:
                self.handle_pong(message)
            elif message_type == Message.FIND_NODE:
                self.handle_find(message)
            elif message_type == Message.FIND_VALUE:
                self.handle_find(message, find_value=True)
            elif message_type == Message.FOUND_NODES:
                self.handle_found_nodes(message)
            elif message_type == Message.FOUND_VALUE:
                self.handle_found_value(message)
            elif message_type == Message.STORE:
                self.handle_store(message)
            client_host, client_port = self.client_address
            peer_id = message[Message.PEER_ID]
            new_peer = Peer(client_host, client_port, peer_id)
            self.server.dht.buckets.insert(new_peer)
        except KeyError:
            pass
        except msgpack.UnpackValueError:
            pass

    def handle_ping(self, message):
        client_host, client_port = self.client_address
        id_ = message[Message.PEER_ID]
        peer = Peer(client_host, client_port, id_)
        peer.pong(
            socket=self.server.socket,
            peer_id=self.server.dht.peer.id,
            lock=self.server.send_lock
        )

    def handle_pong(self, message):
        pass

    def handle_find(self, message, find_value=False):
        key = message[Message.ID]
        id_ = message[Message.PEER_ID]
        client_host, client_port = self.client_address
        peer = Peer(client_host, client_port, id_)
        response_socket = self.request[1]
        if find_value and (key in self.server.dht.data):
            value = self.server.dht.data[key]
            peer.found_value(
                id_,
                value,
                message[Message.RPC_ID],
                socket=response_socket,
                peer_id=self.server.dht.peer.id,
                lock=self.server.send_lock
            )
        else:
            nearest_nodes = self.server.dht.buckets.nearest_nodes(id_)
            if not nearest_nodes:
                nearest_nodes.append(self.server.dht.peer)
            nearest_nodes = [
                nearest_peer.astriple() for nearest_peer in nearest_nodes
            ]
            peer.found_nodes(
                id_,
                nearest_nodes,
                message[Message.RPC_ID],
                socket=response_socket,
                peer_id=self.server.dht.peer.id,
                lock=self.server.send_lock
            )

    def handle_found_nodes(self, message):
        rpc_id = message[Message.RPC_ID]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        nearest_nodes = [Peer(*peer) for peer in message[Message.NEAREST_NODES]]
        shortlist.update(nearest_nodes)

    def handle_found_value(self, message):
        rpc_id = message[Message.RPC_ID]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        shortlist.set_complete(message[Message.VALUE])

    def handle_store(self, message):
        key = message[Message.ID]
        self.server.dht.data[key] = message[Message.VALUE]


class DHTServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    def __init__(self, host_address, handler_cls):
        socketserver.UDPServer.__init__(self, host_address, handler_cls)
        self.send_lock = threading.Lock()


class DHT(object):
    def __init__(self, host, port, id_=None, boot_host=None, boot_port=None):
        if not id_:
            id_ = random_id()
        self.peer = Peer(str(host), port, id_)
        self.data = {}
        self.buckets = BucketSet(k, id_bits, self.peer.id)
        self.rpc_ids = {}  # should probably have a lock for this
        self.server = DHTServer(self.peer.address(), DHTRequestHandler)
        self.server.dht = self
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        self.bootstrap(str(boot_host), boot_port)

    def iterative_find_nodes(self, key, boot_peer=None):
        shortlist = Shortlist(k, key)
        shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
        if boot_peer:
            rpc_id = random_id()
            self.rpc_ids[rpc_id] = shortlist
            boot_peer.find_node(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id)
        start = time.time()
        try:
            while (not shortlist.complete()) or boot_peer:
                boot_peer = None
                nearest_nodes = shortlist.get_next_iteration(alpha)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id = random_id()
                    self.rpc_ids[rpc_id] = shortlist
                    peer.find_node(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id)
                try:
                    shortlist.completion_value.result(timeout=iteration_sleep)
                    return shortlist.results()
                except futures.TimeoutError:
                    pass
            return shortlist.results()
        finally:
            end = time.time()
            # Convert to logging
            print("find_nodes: %.5fs" % (end - start))

    def iterative_find_value(self, key):
        shortlist = Shortlist(k, key)
        shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
        start = time.time()
        try:
            while (not shortlist.complete()):
                nearest_nodes = shortlist.get_next_iteration(alpha)
                for peer in nearest_nodes:
                    shortlist.mark(peer)
                    rpc_id = random_id()
                    self.rpc_ids[rpc_id] = shortlist
                    peer.find_value(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id)
                try:
                    shortlist.completion_value.result(timeout=iteration_sleep)
                    return shortlist.completion_result()
                except futures.TimeoutError:
                    pass
            return shortlist.completion_result()
        finally:
            end = time.time()
            # Convert to logging
            print("find_value: %.5fs" % (end - start))

    def bootstrap(self, boot_host, boot_port):
        if boot_host and boot_port:
            boot_peer = Peer(boot_host, boot_port, 0)
            self.iterative_find_nodes(self.peer.id, boot_peer=boot_peer)

    def __getitem__(self, key):
        hashed_key = hash_function(msgpack.dumps(key))
        if hashed_key in self.data:
            return self.data[hashed_key]
        result = self.iterative_find_value(hashed_key)
        if result:
            return result
        raise KeyError

    def __setitem__(self, key, value):
        hashed_key = hash_function(msgpack.dumps(key))
        nearest_nodes = self.iterative_find_nodes(hashed_key)
        self.data[hashed_key] = value
        for node in nearest_nodes:
            node.store(hashed_key, value, socket=self.server.socket, peer_id=self.peer.id)
        
    def tick():
        pass
