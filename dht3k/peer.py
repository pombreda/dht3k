import hashlib
import msgpack

from .hashing import hash_function
from .const   import Message


class Peer(object):
    ''' DHT Peer Information'''
    def __init__(self, host, port, id_):
        self.host, self.port, self.id = host, port, id_

    def astriple(self):
        return (self.host, self.port, self.id)
        
    def address(self):
        return (self.host, self.port)
        
    def __repr__(self):
        return repr(self.astriple())

    def _sendmessage(self, message, sock=None, peer_id=None, lock=None):
        message[Message.PEER_ID] = peer_id  # more like sender_id
        encoded = msgpack.dumps(message)
        if sock:
            if lock:
                with lock:
                    sock.sendto(encoded, (self.host, self.port))
            else:
                sock.sendto(encoded, (self.host, self.port))
        
    def ping(self, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.PING
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
        
    def pong(self, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.PONG
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
        
    def store(self, key, value, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.STORE,
            Message.ID: key,
            Message.VALUE: value
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
        
    def find_node(self, id, rpc_id, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.FIND_NODE,
            Message.ID: id,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
        
    def found_nodes(self, id, nearest_nodes, rpc_id, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.FOUND_NODES,
            Message.VALUE: id,
            Message.NEAREST_NODES: nearest_nodes,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
        
    def find_value(self, id, rpc_id, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.FIND_VALUE,
            Message.ID: id,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
        
    def found_value(self, id, value, rpc_id, socket=None, peer_id=None, lock=None):
        message = {
            Message.MESSAGE_TYPE: Message.FOUND_VALUE,
            Message.ID: id,
            Message.VALUE: value,
            Message.RPC_ID: rpc_id
        }
        self._sendmessage(message, socket, peer_id=peer_id, lock=lock)
