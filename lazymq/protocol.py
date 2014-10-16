""" Defines the lazymq protocol """

import asyncio
import msgpack
import ipaddress

from .struct     import Message, Connection
from .hashing    import random_id
from .const      import Config, Status
from .log        import l
from .exceptions import BadMessage, ClosedException


class Protocol(object):
    """ Partial/abstract class for the lazymq protocol.

    Needs to be inherited by LazyMQ. """

    def __init__(self):
        """ Init to make lint happy """
        self.encoding     = None
        self.port         = None
        self._connections = None
        self.loop         = None

    @asyncio.coroutine
    def get_connection(
            self,
            port,
            address_v6 = None,
            address_v4 = None,
    ):
        """ Abstract method. Get an active connection to a host. Please provide
        a ipv4- and ipv6-address, you can leave one address None, but its not
        recommended. """
        pass

    def _fill_defaults(self, message):
        """ Fill defaults for fields of message that are None.

        :type message: Message"""
        if not message.identity:
            message.identity = random_id()
        if not message.encoding:
            message.encoding = self.encoding
        if not message.port:
            message.port = self.port

    def _close_conn(self, conn, peer):
        """ Close a connection """
        conn.close()
        # TODO: here late closes can occour after we actually
        # already cleaned up. I think it is ok to just ignore
        # these. But lets think about it later again.
        try:
            del self._connections[peer]
        except KeyError:
            pass
        return

    @asyncio.coroutine
    def _handle_connection(self, reader, writer):
        """ Handles a connection

        :type reader: asyncio.StreamReader
        :type writer: asyncio.StreamWriter """

        peer = writer.get_extra_info('peername')
        conn = None
        try:
            conn = self._connections[peer]
        except KeyError:
            pass
        if not conn:
            conn = Connection(reader, writer)
            self._connections[peer] = conn
        while True:
            try:
                enclenb = yield from reader.readexactly(1)
            except (
                    asyncio.IncompleteReadError,
            ):
                # Before a message has started an incomplete read is ok:
                # it just means the remote has garbage collected the conenction
                self._close_conn(conn, peer)
                return
            try:
                enclen  = int.from_bytes(enclenb, 'big')
                encb    = yield from asyncio.wait_for(
                    reader.readexactly(enclen),
                    Config.TIMEOUT,
                )
                if enclen == 1 and not encb[0]:
                    enc = None
                else:
                    enc  = str(encb, encoding = "ASCII")
                msglenb  = yield from asyncio.wait_for(
                    reader.readexactly(8),
                    Config.TIMEOUT,
                )
                msglen   = int.from_bytes(msglenb, 'big')
                msgb     = yield from asyncio.wait_for(
                    reader.readexactly(msglen),
                    Config.TIMEOUT,
                )
                msg = Message(
                    *msgpack.loads(msgb, encoding=enc)
                )
                addr = ipaddress.ip_address(peer[0])
                if addr.version == 6:
                    msg.address_v4 = None
                    msg.address_v6 = peer[0]
                else:
                    msg.address_v6 = None
                    msg.address_v4 = peer[0]
                msg.port = peer[1]
            except asyncio.IncompleteReadError:
                # Connection was closed
                l.exception(
                    "This should not happend with well behaved clients"
                )
                self._close_conn(conn, peer)
                return
            except asyncio.TimeoutError:
                # Either the message was bad or the peer froze
                # lets send a error and close the connection
                l.exception(
                    "This should not happend with well behaved clients"
                )
                # TODO: send answer (call_later)
                self._close_conn(conn, peer)
                return
            writer.write(
               Status.SUCCESS.to_bytes(1, 'big')
            )

    @asyncio.coroutine
    def deliver(self, message):
        """ Send message and wait for delivery.

        This method is a coroutine. """
        assert isinstance(message, Message)
        self._fill_defaults(message)
        msg = msgpack.dumps(
            message.to_tuple(),
            encoding=message.encoding
        )
        if message.encoding:
            enc     = bytes(message.encoding, encoding = "ASCII")
            enclen  = len(enc)
        else:
            enc     = bytes([0])
            enclen  = 1
        enclenb     = enclen.to_bytes(1, 'big')
        msglen      = len(msg)
        msglenb     = msglen.to_bytes(8, 'big')
        conn = yield from self.get_connection(
            message.port,
            message.address_v6,
            message.address_v4,
        )
        with (yield from conn) as (_, writer):
            writer.write(enclenb)
            writer.write(enc)
            writer.write(msglenb)
            writer.write(msg)
        # TODO: status as exception
