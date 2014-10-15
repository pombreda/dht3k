# pylint: disable=too-many-arguments

""" Lazymq is UDP-semantic over TCP """

import asyncio
import socket
import time
import msgpack

from . import const
from . import hashing

# TODO: add outofthebox SSL support
# TODO: custom SSL cert
# TODO: bandwidth limit
# TODO: does call_soon log exceptions?
# TODO: LazyMQ attrs to ready-only props


class Message(object):
    """ Represents messages to send and received. The address_
    arguments represent the recipient when sending and the sender when
    receiving a message. """
    __slots__ = (
        # Modified by lazymq
        'address_v6',
        'address_v4',
        'status',
        # Passthrough
        'port',
        'encoding',
        'identity',
        'data',
    )

    def __init__(
            self,
            identity   = None,
            data       = None,
            # Take the encoding from LazyMQ
            encoding   = None,
            address_v6 = None,
            address_v4 = None,
            status     = const.Status.SUCCESS,
            # Take the port from LazyMQ
            port       = None,
    ):
        self.identity   = identity
        self.data       = data
        self.encoding   = encoding
        self.address_v6 = address_v6
        self.address_v4 = address_v4
        self.status     = status
        self.port       = port

    def to_tuple(self):
        """ Get the message as tuple to send to the network """
        return (
            self.identity,
            self.data,
            self.encoding,
            self.address_v6,
            self.address_v4,
            self.status,
            self.port,
        )

class LazyMQ(object):
    """ Sending and receiving message TCP without sockets. LazyMQ will handle
    connection for you. It will keep a connection for a while for reuse and
    then clean up the connection. """
    def __init__(
            self,
            port         = const.Config.PORT,
            encoding     = const.Config.ENCODING,
            ip_protocols = const.Config.PROTOS,
            bind_v6      = "",
            bind_v4      = "",
            loop         = None,
    ):
        self.port         = port
        self.encoding     = encoding
        self.ip_protocols = ip_protocols
        self.bind_v6      = bind_v6
        self.bind_v4      = bind_v4
        self._loop        = loop
        self._servers      = []
        self._socks        = []
        self._connections  = {}
        if not self._loop:
            self._loop = asyncio.get_event_loop()
        # Detecting dual_stack sockets seems not to work on some OSs
        # so we always use two sockets
        if ip_protocols & const.Protocols.IPV6:
            sock = socket.socket(
                socket.AF_INET6
            )
            sock.setsockopt(
                socket.IPPROTO_IPV6,
                socket.IPV6_V6ONLY,
                True,
            )
            sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                True,
            )
            sock.bind((bind_v6, port))
            sock.listen(const.Config.BACKLOG)
            server = asyncio.start_server(
                self._handle_connection,
                loop = loop,
                sock = sock,
                backlog = const.Config.BACKLOG,
            )
            self._servers.append(server)
            self._socks.append(sock)
        if ip_protocols & const.Protocols.IPV4:
            sock = socket.socket(
                socket.AF_INET
            )
            sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                True,
            )
            sock.bind((bind_v4, port))
            sock.listen(const.Config.BACKLOG)
            server = asyncio.start_server(
                self._handle_connection,
                loop = loop,
                sock = sock,
                backlog = const.Config.BACKLOG,
            )
            self._servers.append(server)
            self._socks.append(sock)

    def close(self):
        """ Closing everything """
        for sock in self._socks:
            sock.close()
        for server in self._servers:
            server.close()
        for _, _, writer in self._connections.values():
            writer.close()
        self._servers.clear()
        self._socks.clear()
        self._connections.clear()


    @asyncio.coroutine
    def get_connection(
            self,
            port,
            address_v6 = None,
            address_v4 = None,
    ):
        """ Get an active connection to a host. Please provide a ipv4- and
        ipv6-address, you can leave one address None, but its not
        recommended """
        assert address_v4 or address_v6
        try:
            if address_v6:
                return self._connections[(address_v6, port)]
        except KeyError:
            pass
        try:
            if address_v4:
                return self._connections[(address_v4, port)]
        except KeyError:
            pass
        reader = None
        writer = None
        try:
            if address_v6:
                reader, writer = yield from asyncio.open_connection(
                    host = address_v6,
                    port = port,
                    loop = self.loop,
                )
                conn = (
                    time.time(),
                    reader,
                    writer
                )
                self._connections[(address_v6, port)] = conn
                return conn
        except OSError:
            if not address_v4:
                raise
        if address_v4:
            reader, writer = yield from asyncio.open_connection(
                host = address_v4,
                port = port,
                loop = self.loop,
            )
            conn = (
                time.time(),
                reader,
                writer
            )
            self._connections[(address_v4, port)] = conn
            return conn
        raise Exception("This is a bug, please report me to github")

    @property
    def loop(self):
        """ Returns the eventloop used by lazymq

        :rtype: asyncio.AbstractEventLoop """
        return self._loop

    @asyncio.coroutine
    def start(self):
        """ Start everything """
        loop = self.loop
        for server in self._servers:
            loop.async(server)


    @asyncio.coroutine
    def _handle_connection(self, reader, writer):
        """ Handles a connection """

    @asyncio.coroutine
    def deliver(self, message):
        """ Send message and wait for delivery.

        This method is a coroutine"""
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
        try:
            _, reader, writer = self.get_connection(
                message.port,
                message.address_v6,
                message.address_v4,
            )
            writer.write(enclenb)
            writer.write(enc)
            writer.write(msglenb)
            writer.write(msg)
            status = reader.readexactly(1)
        except OSError:
            pass # TODO: catch and send all erros (local)
        # TODO: send status (local)
        print("huhu")


    def send(self, message):
        """ Send message """
        self._fill_defaults(message)

    @asyncio.coroutine
    def receive(self):
        """ Receive message.

        This methid is a coroutine"""
        pass

    def _fill_defaults(self, message):
        """ Fill defaults for fields of message that are None.

        :type message: Message"""

        if not message.identity:
            message.identity = hashing.random_id()
        if not message.encoding:
            message.encoding = self.encoding
        if not message.port:
            message.port = self.port
