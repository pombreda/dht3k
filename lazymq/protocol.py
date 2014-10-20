""" Defines the lazymq protocol """

import asyncio
import msgpack
import ipaddress
import traceback
import random

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
        self._future      = None
        self._received    = None

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
        msg  = None
        try:
            conn = self._connections[peer]
        except KeyError:
            pass
        if not conn:
            l.debug("Connction opened: %s", conn)
            conn = Connection(reader, writer)
            self._connections[peer] = conn
        else:
            l.debug("Handling existing conn: %s", conn)
        while True:
            try:
                enclenb = yield from reader.readexactly(1)
            except (
                    asyncio.IncompleteReadError,
            ):
                # Before a message has started an incomplete read is ok:
                # it just means the remote has garbage collected the conenction
                l.debug("Idle connection closed: %s", conn)
                self._close_conn(conn, peer)
                return
            try:
                l.debug("Begin receiving: %s", conn)
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
                self.send_error(peer, traceback.format_exc)
                self._close_conn(conn, peer)
                return
            except msgpack.exceptions.ExtraData:
                self.send_error(peer, traceback.format_exc)
                self._close_conn(conn, peer)
                return
            except msgpack.UnpackException:
                self.send_error(peer, traceback.format_exc)
                self._close_conn(conn, peer)
                return
            self._future.set_result(msg)
            yield from asyncio.wait_for(
                self._received.wait(),
                timeout=Config.TIMEOUT,
            )
            self._received.clear()
            self._future = asyncio.Future(loop=self.loop)
            l.debug("Receive completed: %s", conn)

    def send_error(self, peer, exception):
        """ Send error on exceptions """
        msg = Message(
            port = peer[1],
            status = Status.BAD_MESSAGE,
            # TODO: Is there a way to find out the encoding?
            encoding = "UTF-8",
            data = exception
        )
        addr = ipaddress.ip_address(peer[0])
        if addr.version == 6:
            msg.address_v4 = None
            msg.address_v6 = peer[0]
        else:
            msg.address_v6 = None
            msg.address_v4 = peer[0]
        asyncio.async(self._devliver_error(msg))

    @asyncio.coroutine
    def _devliver_error(self, msg):
        """ Coroutine to deliver a error """
        wait = (random.random() / 2) + 0.5
        yield from asyncio.sleep(wait)
        yield from self.deliver(msg, True)


    @asyncio.coroutine
    def deliver(self, message, is_error=False):
        """ Send message and wait for delivery.

        This method is a coroutine. """
        assert isinstance(message, Message)
        l.debug("Sending message to: %s", (
            message.address_v6,
            message.address_v4,
            message.port,
        ))
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
        l.debug("Got connection: %s", conn)
        with (yield from conn) as (_, writer):
            writer.write(enclenb)
            writer.write(enc)
            writer.write(msglenb)
            writer.write(msg)
        l.debug("Wrote message to stream: %s", conn)
