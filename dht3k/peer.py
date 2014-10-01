import msgpack
import ipaddress
import six

from .hashing   import hash_function
from .const     import Message, MinMax
from .helper    import sixunicode
from .excepions import MaxSizeException
from .log       import l


class Peer(object):
    ''' DHT Peer Information'''
    def __init__(
            self,
            port,
            id_,
            hostv4         = None,
            hostv6         = None,
            well_connected = False,
            is_bytes       = False
    ):
        if hostv4:
            self.hostv4 = ipaddress.ip_address(
                sixunicode(hostv4, is_bytes)
            )
        else:
            self.hostv4 = None
        if hostv6:
            self.hostv6 = ipaddress.ip_address(
                sixunicode(hostv6, is_bytes)
            )
        else:
            self.hostv6 = None
        self.port = port
        self.id = id_
        self.well_connected = well_connected

    def astuple(self, for_export=False):
        if self.hostv4:
            hostv4 = self.hostv4.packed
        else:
            hostv4 = None
        if self.hostv6:
            hostv6 = self.hostv6.packed
        else:
            hostv6 = None
        if for_export:
            return (
                self.port,
                self.id,
                hostv4,
                hostv6,
            )
        else:
            return (
                self.port,
                self.id,
                hostv4,
                hostv6,
                self.well_connected,
            )

    def addressv4(self):
        return (str(self.hostv4), self.port)

    def addressv6(self):
        return (str(self.hostv6), self.port)

    def __repr__(self):
        return repr(self.astuple())

    def _sendmessage(self, message, dht, peer_id):
        message[Message.PEER_ID] = peer_id  # more like sender_id
        message[Message.NETWORK_ID] = hash_function(
            peer_id + dht.network_id
        )
        encoded = msgpack.dumps(message)
        if len(encoded) > MinMax.MAX_MSG_SIZE:
            raise MaxSizeException(
                "Message size max not exceed %d bytes" % MinMax.MAX_MSG_SIZE
            )
        if self.hostv4 and dht.server4:
            try:
                dht.server4.socket.sendto(
                    encoded,
                    (str(self.hostv4), self.port)
                )
            except OSError:
                l.info("Could not send to %s", self.hostv4)
        if self.hostv6 and dht.server6:
            try:
                dht.server6.socket.sendto(
                    encoded,
                    (str(self.hostv6), self.port)
                )
            except OSError:
                l.info("Could not send to %s", self.hostv6)

    def _fw_sendmessage(self, message, dht, peer_id):
        message[Message.PEER_ID] = peer_id  # more like sender_id
        message[Message.NETWORK_ID] = hash_function(
            peer_id + dht.network_id
        )
        encoded = msgpack.dumps(message)
        if len(encoded) > MinMax.MAX_MSG_SIZE:
            raise MaxSizeException(
                "Message size max not exceed %d bytes" % MinMax.MAX_MSG_SIZE
            )
        if self.hostv4 and dht.server4:
            dht.fw_sock4.sendto(
                encoded,
                (str(self.hostv4), self.port)
            )
        if self.hostv6 and dht.server6:
            dht.fw_sock6.sendto(
                encoded,
                (str(self.hostv6), self.port)
            )

    def ping(self, dht, peer_id, rpc_id=None):
        message = {
            Message.MESSAGE_TYPE: Message.PING,
            Message.ALL_ADDR: self.astuple(
                for_export=True
            ),
        }
        if rpc_id:
            message[Message.RPC_ID] = rpc_id
        self._sendmessage(message, dht, peer_id=peer_id)

    def fw_ping(self, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.FW_PING,
        }
        self._sendmessage(message, dht, peer_id=peer_id)

    def pong(self, dht, peer_id, cpeer, rpc_id=None):
        message = {
            Message.MESSAGE_TYPE: Message.PONG,
            Message.ALL_ADDR: dht.peer.astuple(
                for_export=True
            ),
            Message.CLI_ADDR: cpeer.astuple(
                for_export=True
            ),
        }
        if rpc_id:
            message[Message.RPC_ID] = rpc_id
        self._sendmessage(message, dht, peer_id=peer_id)

    def fw_pong(self, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.FW_PONG,
            Message.ID: self.id,
        }
        self._fw_sendmessage(message, dht, peer_id=peer_id)

    def store(self, key, value, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.STORE,
            Message.ID: key,
            Message.VALUE: value
        }
        self._sendmessage(message, dht, peer_id=peer_id)

    def find_node(self, id_, rpc_id, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.FIND_NODE,
            Message.ID: id_,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, dht, peer_id=peer_id)

    def found_nodes(self, id_, nearest_nodes, rpc_id, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.FOUND_NODES,
            Message.VALUE: id_,
            Message.NEAREST_NODES: nearest_nodes,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, dht, peer_id=peer_id)

    def find_value(self, id_, rpc_id, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.FIND_VALUE,
            Message.ID: id_,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, dht, peer_id=peer_id)

    def found_value(self, id_, value, rpc_id, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.FOUND_VALUE,
            Message.ID: id_,
            Message.VALUE: value,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, dht, peer_id=peer_id)
