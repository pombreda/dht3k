import msgpack
import socket
import ipaddress
import six
import concurrent.futures as futures
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
import threading
import time

from .bucketset import BucketSet
from .hashing   import hash_function, random_id, id_bytes, int2bytes
from .peer      import Peer
from .shortlist import Shortlist
from .const     import Message
from .helper    import sixunicode

k = 20
alpha = 3
id_bits = id_bytes * 8
iteration_sleep = 1

Shortlist.iteration_sleep = iteration_sleep


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
            peer_id = message[Message.PEER_ID]
            new_peer = self.peer_from_client_address(
                self.client_address,
                peer_id
            )
            self.server.dht.buckets.insert(
                new_peer,
                self.server,
                message_type == Message.PONG
            )
        except KeyError:
            pass
        except msgpack.UnpackValueError:
            pass

    def peer_from_client_address(self, client_address, id_):
        ipaddr = ipaddress.ip_address(
            sixunicode(client_address[0])
        )
        if isinstance(ipaddr, ipaddress.IPv6Address):
            return Peer(client_address[1], id_, hostv6=ipaddr)
        else:
            return Peer(client_address[1], id_, hostv4=ipaddr)

    def handle_ping(self, message):
        print("ping")
        id_ = message[Message.PEER_ID]
        cpeer = self.peer_from_client_address(self.client_address, id_)
        cpeer.pong(
            dht=self.server.dht,
            peer_id=self.server.dht.peer.id,
        )
        apeer = Peer(
            *message[Message.ALL_ADDR],
            is_bytes=True
        )
        apeer.pong(
            dht=self.server.dht,
            peer_id=self.server.dht.peer.id,
        )

    def handle_pong(self, message):
        print("pong")

    def handle_find(self, message, find_value=False):
        key = message[Message.ID]
        id_ = message[Message.PEER_ID]
        peer = self.peer_from_client_address(self.client_address, id_)
        response_socket = self.request[1]
        if find_value and (key in self.server.dht.data):
            value = self.server.dht.data[key]
            peer.found_value(
                id_,
                value,
                message[Message.RPC_ID],
                dht=self.server.dht,
                peer_id=self.server.dht.peer.id,
            )
        else:
            nearest_nodes = self.server.dht.buckets.nearest_nodes(id_)
            if not nearest_nodes:
                nearest_nodes.append(self.server.dht.peer)
            nearest_nodes = [
                nearest_peer.astuple() for nearest_peer in nearest_nodes
            ]
            peer.found_nodes(
                id_,
                nearest_nodes,
                message[Message.RPC_ID],
                dht=self.server.dht,
                peer_id=self.server.dht.peer.id,
            )

    def handle_found_nodes(self, message):
        rpc_id = message[Message.RPC_ID]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        nearest_nodes = [Peer(
            *peer,
            is_bytes=True
        ) for peer in message[Message.NEAREST_NODES]]
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
    def __init__(self, host_address, handler_cls, is_v6=False):
        if is_v6:
            socketserver.UDPServer.address_family = socket.AF_INET6
        else:
            socketserver.UDPServer.address_family = socket.AF_INET
        socketserver.UDPServer.__init__(
            self,
            host_address,
            handler_cls,
        )
        self.send_lock = threading.Lock()


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
