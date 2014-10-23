# pylint: disable=too-many-arguments

""" Lazymq is UDP-semantic over TCP """

import asyncio
import socket
import time

from .         import const
from .struct   import Connection, Message
from .protocol import Protocol
from .log      import l
from .crypt    import LinkEncryption
from .tasks    import Cleanup


# TODO: connection cleanup
# TODO: what if one only sends through connection? (no refresh)
# TODO: close during send is not good
#   -> probably refresh on begin of operation (send/recv
# TODO: test reuse
# TODO: custom SSL cert
# TODO: bandwidth limit
# TODO: does call_soon log exceptions?
# TODO: LazyMQ attrs to ready-only props
# TODO: documentation, more tests, refactoring, review



class LazyMQ(Protocol, LinkEncryption, Cleanup):
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
        self._future       = asyncio.Future(loop=self.loop)
        self._future_lock  = asyncio.Lock(loop=self.loop)
        self._waiters      = 0
        self._received     = asyncio.Event(loop=self.loop)
        self._queue        = asyncio.Queue(loop=self.loop)
        self._closed       = asyncio.Event(loop=self.loop)
        self.setup_tls()
        if not self._loop:
            self._loop = asyncio.get_event_loop()
        if ip_protocols & const.Protocols.IPV6:
            self._start_server(socket.AF_INET6, bind_v6)
        if ip_protocols & const.Protocols.IPV4:
            self._start_server(socket.AF_INET, bind_v4)
        asyncio.async(
            self.run_cleanup(),
            loop=self._loop,
        )
        l.debug("LazyMQ set up")

    @property
    def queue(self):
        """ Return the queue.

        :rtype: asyncio.Queue """
        return self._queue

    def _start_server(
            self,
            family,
            bind,
    ):
        """ Starts a server """
        # Detecting dual_stack sockets seems not to work on some OSs
        # so we always use two sockets
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
            loop    = self._loop,
            sock    = sock,
            backlog = const.Config.BACKLOG,
            ssl     = self._ssl_context,
        )
        l.debug("Server created: %s", server)
        self._servers.append(server)
        self._socks.append(sock)

    @asyncio.coroutine
    def close(self):
        """ Closing everything """
        self._closed.set()
        for server in self._servers:
            server.close()
            yield from server.wait_closed()
            # Give active reads a chance to die
            yield from asyncio.sleep(0.1)
        for sock in self._socks:
            sock.close()
        for conn in self._connections.values():
            conn.close()
        self._servers.clear()
        self._socks.clear()
        self._connections.clear()
        if not self.loop.is_running():
            self.loop.run_until_complete(asyncio.sleep(0))

    @asyncio.coroutine
    def _do_open(self, port, address):
        """ Open a connection with a defined timeout """
        reader, writer = yield from asyncio.wait_for(
            asyncio.open_connection(
                host = str(address),
                port = port,
                loop = self.loop,
                ssl=self._ssl_context
            ),
            const.Config.TIMEOUT,
        )

        conn = Connection(
            reader,
            writer,
        )
        handler = self._handle_connection(reader, writer, conn)
        asyncio.async(
            handler,
            loop = self.loop,
        )
        peer = writer.get_extra_info('peername')
        peer = self._make_connection_key(peer[0], peer[1])
        # for consistency get the peername from the socket!
        self._connections[peer] = conn
        return conn

    @asyncio.coroutine
    def get_connection(
            self,
            port,
            active_port = None,
            address_v6  = None,
            address_v4  = None,
    ):
        """ Get an active connection to a host. Please provide a ipv4- and
        ipv6-address, you can leave one address None, but its not
        recommended. """
        assert address_v4 or address_v6
        port = int(port)
        if active_port:
            active_port = int(active_port)
            try:
                if address_v6:
                    return self._connections[(address_v6.packed, active_port)]
            except KeyError:
                pass
            try:
                if address_v4:
                    return self._connections[(address_v4.packed, active_port)]
            except KeyError:
                pass
        try:
            if address_v6:
                return self._connections[(address_v6.packed, port)]
        except KeyError:
            pass
        try:
            if address_v4:
                return self._connections[(address_v4.packed, port)]
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

    @asyncio.coroutine
    def start(self):
        """ Start everything """
        newservers = []
        for server in self._servers:
            newservers.append((yield from server))
        self._servers = newservers


    def send(self, message):
        """ Send message """
        self._fill_defaults(message)

    @asyncio.coroutine
    def receive(self):
        """ Receive a message from the queue """
        return (yield from self._queue.get())

    @asyncio.coroutine
    def communicate(
            self,
            message,
            timeout=const.Config.TIMEOUT,
    ):
        """ Deliver a message and wait for an answer. The identity of the
        answer has to be same as the request. IMPORTANT: The message will still
        be delivered to the queue, so please consume the message.

        This method is a coroutine. """
        self._waiters += 1
        # l.debug("Starting to communicate")
        yield from self.deliver(message)
        while True:
            try:
                result = yield from asyncio.wait_for(
                    self._future,
                    timeout=timeout,
                    loop=self._loop,
                )
                # l.debug("Got a message")
                if result.identity == message.identity:
                    return result
            finally:
                self._waiters -= 1
                if not self._waiters:
                    self._future = asyncio.Future(loop=self.loop)
                    self._received.set()
            self._waiters += 1

