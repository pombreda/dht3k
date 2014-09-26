""" Module for the server """
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
import socket
import threading
import msgpack
import ipaddress

from .const     import Message
from .helper    import sixunicode
from .peer      import Peer


class DHTRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        try:
            message      = msgpack.loads(self.request[0].strip())
            message_type = message[Message.MESSAGE_TYPE]
            is_pong      = False
            is_rpc_ping  = False

            if message_type == Message.PING:
                self.handle_ping(message)
            elif message_type == Message.PONG:
                is_pong = True
                is_rpc_ping = self.handle_pong(message)
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
            # Prevent DoS attack: flushing of bucket
            if is_pong and not is_rpc_ping:
                return
            new_peer = self.peer_from_client_address(
                self.client_address,
                peer_id
            )
            self.server.dht.buckets.insert(
                new_peer,
                self.server,
                is_pong
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
        try:
            rpc_id = message[Message.RPC_ID]
        except KeyError:
            rpc_id = None

        cpeer = self.peer_from_client_address(self.client_address, id_)
        apeer = Peer(
            *message[Message.ALL_ADDR],
            is_bytes=True
        )
        apeer.pong(
            dht     = self.server.dht,
            peer_id = self.server.dht.peer.id,
            cpeer   = cpeer,
            rpc_id  = rpc_id,
        )
        if    (cpeer.addressv4 == apeer.addressv4 or
               cpeer.addressv6 == apeer.addressv6):
            return
        cpeer.pong(
            dht     = self.server.dht,
            peer_id = self.server.dht.peer.id,
            cpeer   = cpeer,
            rpc_id  = rpc_id,
        )

    def handle_pong(self, message):
        try:
            rpc_id = message[Message.RPC_ID]
            self.server.dht.rpc_ids[rpc_id].append(
                message
            )
            return True
        except KeyError:
            return False

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
            False,
        )
        if is_v6:
            try:
                self.socket.setsockopt(
                    socket.IPPROTO_IPV6,
                    socket.IPV6_V6ONLY,
                    True,
                )
            except socket.error:
                pass
        self.server_bind()
        self.server_activate()
        self.send_lock = threading.Lock()
