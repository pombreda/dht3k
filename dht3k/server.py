""" Module for the server """
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
import socket
import threading
import msgpack
import ipaddress

from .const     import Message, MinMax, Config, message_dict
from .helper    import sixunicode
from .peer      import Peer
from .threads   import ThreadPoolMixIn
from .log       import l


def _get_lookup():
    """ Create lookup """

    def print_len(obj):  # noqa
        """ Helper for creating verifiers """
        print("Len: %d" % len(obj))
        return True

    def print_obj(obj):  # noqa
        """ Helper for creating verifiers """
        print("Obj: %s" % obj)
        return True

    def verify_ip(ip):
        """ Check this can be an IP """
        ip_len = len(ip)
        return ip_len <= MinMax.MAX_IP_LEN and ip_len >= MinMax.MIN_IP_LEN

    def verify_boot_peer(peer):
        """ Verify if this can be an Peer """
        if peer[0] > 2 ** 32:
            return False
        if peer[0] < 1024:
            return False
        if peer[1] and len(peer[1]) != Config.ID_BYTES:
            return False
        if peer[2] and not verify_ip(peer[2]):
            return False
        if peer[3] and not verify_ip(peer[3]):
            return False
        return True

    def verify_peer(peer):
        """ Verify if this can be an Peer """
        if peer[0] > 2 ** 32:
            return False
        if peer[0] < 1024:
            return False
        if len(peer[1]) != Config.ID_BYTES:
            return False
        if peer[2] and not verify_ip(peer[2]):
            return False
        if peer[3] and not verify_ip(peer[3]):
            return False
        return True

    def verify_nodes(nodes):
        """ Check if all nodes kann be peers """
        for node in nodes:
            if not verify_peer(node):
                return False
        return True

    return {
        Message.PEER_ID: lambda x: len(x) == Config.ID_BYTES,
        Message.RPC_ID:  lambda x: len(x) == Config.ID_BYTES,
        Message.ID:  lambda x: len(x) == Config.ID_BYTES,
        Message.CLI_ADDR: verify_ip,
        Message.ALL_ADDR: verify_boot_peer,
        Message.NEAREST_NODES: verify_nodes,
    }

_verifier_lookup = _get_lookup()


class DHTRequestHandler(socketserver.BaseRequestHandler):

    def verify_message(self, message):
        if message[Message.MESSAGE_TYPE] not in message_dict:
            l.warn("Unknown message type, ignoring message")
            return False
        for key in message.keys():
            if key not in message_dict:
                l.warn("Unknown message part, ignoring message")
                return False
            try:
                verfier = _verifier_lookup[key]
            except KeyError:
                continue
            if not verfier(message[key]):
                l.warn(
                    "Unable to verify: %s, ignoring message", (
                        message_dict[key]
                    )
                )
                return False
        return True

    def handle(self):
        try:
            data         = self.request[0].strip()
            if len(data) > MinMax.MAX_MSG_SIZE:
                l.warn("Message size too large, ignoring message")
                return
            message      = msgpack.loads(
                data,
            )
            if not self.verify_message(message):
                return
            message_type = message[Message.MESSAGE_TYPE]
            is_pong      = False
            is_rpc_ping  = False

            if message_type == Message.PING:
                self.handle_ping(message)
            elif message_type == Message.PONG:
                is_pong = True
                is_rpc_ping = self.handle_pong(message)
            elif message_type == Message.FW_PING:
                self.handle_fw_ping(message)
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
            elif message_type == Message.FW_PONG:
                self.handle_fw_pong(message)
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
        id_ = message[Message.PEER_ID]
        try:
            rpc_id = message[Message.RPC_ID]
        except KeyError:
            rpc_id = None

        l.info("Ping from %s", self.client_address)
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

    def handle_fw_ping(self, message):
        id_ = message[Message.PEER_ID]
        peer = self.peer_from_client_address(self.client_address, id_)
        l.info("Fw ping from %s", self.client_address)
        peer.fw_pong(self.server.dht)

    def handle_fw_pong(self, message):
        id_ = message[Message.ID]
        if id_ == self.server.dht.peer.id:
            self.server.dht.firewalled = False
            l.info("Nolonger marked as firewalled")

    def handle_pong(self, message):
        try:
            rpc_id = message[Message.RPC_ID]
            with self.server.dht.rpc_states as states:
                states[rpc_id].append(
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
        with self.server.dht.data as data:
            if find_value and (key in data):
                value = data[key]
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
        with self.server.dht.rpc_states as states:
            shortlist = states[rpc_id]
            del states[rpc_id]
            nearest_nodes = [Peer(
                *peer,
                is_bytes=True
            ) for peer in message[Message.NEAREST_NODES]]
            shortlist.update(nearest_nodes)

    def handle_found_value(self, message):
        rpc_id = message[Message.RPC_ID]
        with self.server.dht.rpc_states as states:
            shortlist = states[rpc_id]
            del states[rpc_id]
            shortlist.set_complete(message[Message.VALUE])

    def handle_store(self, message):
        key = message[Message.ID]
        with self.server.dht.data as data:
            data[key] = message[Message.VALUE]


class DHTServer(ThreadPoolMixIn, socketserver.UDPServer):
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
