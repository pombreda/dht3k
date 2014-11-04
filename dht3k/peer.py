import ipaddress
import lazymq
import asyncio

from .hashing   import hash_function
from .const     import Message, MinMax
from .log       import l


class Peer(object):
    ''' DHT Peer Information'''
    def __init__(
            self,
            port,
            id_,
            host_v4        = None,
            host_v6        = None,
            well_connected = False,
    ):
        if host_v4:
            self.host_v4 = ipaddress.ip_address(
                host_v4
            )
        else:
            self.host_v4 = None
        if host_v6:
            self.host_v6 = ipaddress.ip_address(
                host_v6
            )
        else:
            self.host_v6 = None
        self.port = port
        self.id = id_
        self.well_connected = well_connected

    def astuple(self, for_export=False):
        if self.host_v4:
            host_v4 = self.host_v4.packed
        else:
            host_v4 = None
        if self.host_v6:
            host_v6 = self.host_v6.packed
        else:
            host_v6 = None
        if for_export:
            return (
                self.port,
                self.id,
                host_v4,
                host_v6,
            )
        else:
            return (
                self.port,
                self.id,
                host_v4,
                host_v6,
                self.well_connected,
            )

    def address_v4(self):
        return (str(self.host_v4), self.port)

    def address_v6(self):
        return (str(self.host_v6), self.port)

    def __repr__(self):
        return repr(self.astuple())

    @asyncio.coroutine
    def _sendmessage(self, message, dht):
        message[Message.PEER_ID] = dht.peer.id  # more like sender_id
        msg = lazymq.Message(
            data = message,
            encoding = dht.encoding,
            address_v4 = self.address_v4,
            address_v6 = self.address_v6,
        )
        yield from dht.lazymq.deliver(msg)
        return msg

    @asyncio.coroutine
    def ping(self, dht):
        message = {
            Message.MESSAGE_TYPE: Message.PING,
            Message.ALL_ADDR: self.astuple(
                for_export=True
            ),
            Message.PEER_ID: dht.peer.id,
        }
        msg = lazymq.Message(
            data = message,
            encoding = dht.encoding,
            address_v4 = self.address_v4,
            address_v6 = self.address_v6,
        )
        yield from dht.lazymq.communicate(msg)
        return msg

    def store(self, key, value, dht):
        message = {
            Message.MESSAGE_TYPE: Message.STORE,
            Message.ID: key,
            Message.VALUE: value
        }
        return self._sendmessage(message, dht)

    def find_node(self, id_, dht):
        message = {
            Message.MESSAGE_TYPE: Message.FIND_NODE,
            Message.ID: id_,
        }
        return self._sendmessage(message, dht)

    def found_nodes(self, id_, nearest_nodes, dht):
        message = {
            Message.MESSAGE_TYPE: Message.FOUND_NODES,
            Message.VALUE: id_,
            Message.NEAREST_NODES: nearest_nodes,
        }
        return self._sendmessage(message, dht)

    def find_value(self, id_, dht):
        message = {
            Message.MESSAGE_TYPE: Message.FIND_VALUE,
            Message.ID: id_,
        }
        return self._sendmessage(message, dht)

    def found_value(self, id_, value, dht):
        message = {
            Message.MESSAGE_TYPE: Message.FOUND_VALUE,
            Message.ID: id_,
            Message.VALUE: value,
        }
        return self._sendmessage(message, dht)
