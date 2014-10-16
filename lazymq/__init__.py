# pylint: disable=too-many-arguments

""" Lazymq is UDP-semantic over TCP """

import asyncio
import socket
import time

from .         import const
from .struct   import Connection, Message
from .protocol import Protocol


# TODO: receive special key (id, value) id: id, connection
# TODO: One retry with random sleep between 0.0 and 1.0 seconds
# TODO: think about locking
# TODO: test reuse
# TODO: close connection after reporting/receiving BadStream
# TODO: handle closing of connections (do a close connection method and refact)
# TODO: return code + message and let deliver raise exceptions ie BadStream
# TODO: receive check bad message and return bad message status
# TODO: bad message can mean a read timeout catch this
# TODO: read time in deliver: return bad peer status
# TODO: add outofthebox SSL support
# TODO: custom SSL cert
# TODO: bandwidth limit
# TODO: does call_soon log exceptions?
# TODO: LazyMQ attrs to ready-only props



class LazyMQ(Protocol):
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
            self._start_server(socket.AF_INET6, bind_v6)
        if ip_protocols & const.Protocols.IPV4:
            self._start_server(socket.AF_INET, bind_v4)

    def _start_server(
            self,
            family,
            bind,
    ):
        """ Starts a server """
        sock = socket.socket(family)
        if family is socket.AF_INET6:
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
        sock.bind((bind, self.port))
        sock.listen(const.Config.BACKLOG)
        server = asyncio.start_server(
            self._handle_connection,
            loop = self._loop,
            sock = sock,
            backlog = const.Config.BACKLOG,
        )
        self._servers.append(server)
        self._socks.append(sock)

    def close(self):
        """ Closing everything """
        for server in self._servers:
            server.close()
        for sock in self._socks:
            sock.close()
        for conn in self._connections.values():
            conn.close()
        self._servers.clear()
        self._socks.clear()
        self._connections.clear()

    @asyncio.coroutine
    def _do_open(self, port, address):
        """ Open a connection with a defined timeout """
        reader, writer = yield from asyncio.wait_for(
            asyncio.open_connection(
                host = address,
                port = port,
                loop = self.loop,
            ),
            const.Config.TIMEOUT,
        )
        conn = Connection(
            reader,
            writer
        )
        peer = writer.get_extra_info('peername')
        # for consistency get the peername from the socket!
        self._connections[peer] = conn
        asyncio.async(self._handle_connection(reader, writer))
        return conn

    @asyncio.coroutine
    def get_connection(
            self,
            port,
            address_v6 = None,
            address_v4 = None,
    ):
        """ Get an active connection to a host. Please provide a ipv4- and
        ipv6-address, you can leave one address None, but its not
        recommended. """
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
        try:
            if address_v6:
                return (yield from self._do_open(port, address_v6))
        except OSError:
            if not address_v4:
                raise
        if address_v4:
            return (yield from self._do_open(port, address_v4))
        raise Exception("I am a bug, please report me on github")

    @property
    def loop(self):
        """ Returns the eventloop used by lazymq.

        :rtype: asyncio.AbstractEventLoop """
        return self._loop

    def start(self):
        """ Start everything """
        loop = self.loop
        for server in self._servers:
            asyncio.async(server)


    def send(self, message):
        """ Send message """
        self._fill_defaults(message)

    @asyncio.coroutine
    def receive(self):
        """ Receive message.

        This methid is a coroutine. """
        pass

