import msgpack
import ipaddress
import six

from .hashing import hash_function
from .const   import Message
from .helper  import sixunicode


class Peer(object):
    ''' DHT Peer Information'''
    def __init__(self, port, id_, hostv4=None, hostv6=None):
        if hostv4:
            self.hostv4 = ipaddress.ip_address(
                sixunicode(hostv4)
            )
        else:
            self.hostv4 = None
        if hostv6:
            self.hostv6 = ipaddress.ip_address(
                sixunicode(hostv6)
            )
        else:
            self.hostv6 = None
        self.port = port
        self.id = id_

    def astuple(self):
        if self.hostv4:
            hostv4 = self.hostv4.packed
        else:
            hostv4 = None
        if self.hostv6:
            hostv6 = self.hostv6.packed
        else:
            hostv6 = None
        return (
            self.port,
            self.id,
            hostv4,
            hostv6,
        )

    def addressv4(self):
        return (str(self.hostv4), self.port)

    def addressv6(self):
        return (str(self.hostv6), self.port)
        
    def __repr__(self):
        return repr(self.astriple())

    def _sendmessage(self, message, dht, peer_id):
        message[Message.PEER_ID] = peer_id  # more like sender_id
        encoded = msgpack.dumps(message)
        if self.hostv4:
            dht.server4.socket.sendto(
                encoded,
                (str(self.hostv4), self.port)
            )
        if self.hostv6:
            dht.server6.socket.sendto(
                encoded,
                (str(self.hostv6), self.port)
            )

    def ping(self, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.PING
        }
        self._sendmessage(message, dht, peer_id=peer_id)

    def pong(self, dht, peer_id):
        message = {
            Message.MESSAGE_TYPE: Message.PONG
        }
        self._sendmessage(message, dht, peer_id=peer_id)

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
