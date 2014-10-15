# pylint: disable=too-many-arguments

""" Lazymq is UDP-semantic over TCP """

import asyncio
import socket

from . import const
from . import hashing


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

class LazyMQ(object):
    """ Sending and receiving message TCP without sockets. LazyMQ will handle
    connection for you. It will keep a connection for a while for reuse and
    then clean up the connection. """
    def __init__(
            self,
            port = const.Config.PORT,
            encoding = const.Config.ENCODING,
            ip_protocols = const.Config.PROTOS,
            bindv4 = "",
            bindv6 = "",
            loop = None,
    ):
        self.port         = port
        self.encoding     = encoding
        self.ip_protocols = ip_protocols
        self.bindv4       = bindv4
        self.bindv6       = bindv6
        self._loop        = loop
        self.servers      = []
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
            sock.bind((bindv6, port))
            sock.listen(const.Config.BACKLOG)
            server = asyncio.start_server(
                self._handle_connection,
                host = bindv6,
                port = port,
                loop = loop,
                sock = sock,
                backlog = const.Config.BACKLOG,
            )
            self.servers.append(server)
        if ip_protocols & const.Protocols.IPV4:
            sock = socket.socket(
                socket.AF_INET
            )
            sock.bind((bindv6, port))
            sock.listen(const.Config.BACKLOG)
            server = asyncio.start_server(
                self._handle_connection,
                host = bindv4,
                port = port,
                loop = loop,
                sock = sock,
                backlog = const.Config.BACKLOG,
            )
            self.servers.append(server)

    @property
    def loop(self):
        """ Returns the eventloop used by lazymq

        :rtype: asyncio.AbstractEventLoop """
        return self._loop

    @asyncio.coroutine
    def start(self):
        """ Start everything """
        loop = self.loop
        for server in self.servers:
            loop.call_soon(server)


    @asyncio.coroutine
    def _handle_connection(self, reader, writer):
        """ Handles a connection """

    @asyncio.coroutine
    def deliver(self, message):
        """ Send message and wait for delivery.

        This method is a coroutine"""
        self._fill_defaults(message)

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
