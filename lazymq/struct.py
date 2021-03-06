""" Datastructures that are either used for wireprotocol or that have to be
efficient """

import asyncio
import time
import ipaddress

from .const      import Status, Config


class _ContextManager(object):
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    lock around a block:

        with (yield from conn):
            <block>

    while failing loudly when accidentally using:

        with lock:
            <block>
    """
    __slots__ = ('_conn',)

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        """ Return the reader and writer of the connection """
        conn = self._conn
        return (conn._reader, conn._writer)

    def __exit__(self, *args):
        try:
            self._conn._lock.release()
        finally:
            self._conn = None  # Crudely prevent reuse.

class Connection(object):
    """ Represents a connection: reader, writer, timestamp and lock. """
    __slots__ = (
        '_reader',
        '_writer',
        '_lock',
        '_timestamp',
        '_handler',
        'handshake_event',
    )

    def __init__(
            self,
            reader,
            writer,
    ):
        self._reader    = reader
        self._writer    = writer
        self._lock      = asyncio.Lock()
        self._timestamp = time.time()
        self.handshake_event = asyncio.Event()

    def __repr__(self):
        peer = self._writer.get_extra_info('peername')
        sock = self._writer.get_extra_info('sockname')
        return "%s -> %s" % (repr(sock), repr(peer))

    def refresh(self):
        """ Refresh the timestamp to prolong garbage collection """
        self._timestamp = time.time()

    def __iter__(self):
        """ Lock and reader/writer """
        yield from self._lock.acquire()
        return _ContextManager(self)

    def __enter__(self):
        """ Warn about misuse """
        raise RuntimeError(
            '"yield from" should be used as context manager expression'
        )

    def __exit__(self, type_, value, traceback):
        """ Needs to exist """
        pass

    def close(self):
        """ Close the connection """
        self._writer.close()

    def collect(self):
        """ If the connection needs to be collected it will be closed and
        True is returned """
        if (time.time() - self._timestamp) > Config.REUSE_TIME:
            self._writer.close()
            return True
        return False


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
        # Private
        'active_port',
    )

    def __init__(
            self,
            identity   = None,
            data       = None,
            # Take the encoding from LazyMQ
            encoding   = None,
            address_v6 = None,
            address_v4 = None,
            status     = Status.SUCCESS,
            # Take the port from LazyMQ
            port       = None,
    ):
        self.identity    = identity
        self.data        = data
        self.encoding    = encoding
        self.address_v6  = address_v6
        self.address_v4  = address_v4
        self.status      = status
        self.port        = port
        self.active_port = None


    def address_v4_packed(self):
        """ Get packed address """
        if self.address_v4 is None:
            return None
        else:
            return ipaddress.ip_address(self.address_v4).packed


    def address_v6_packed(self):
        """ Get packed address """
        if self.address_v6 is None:
            return None
        else:
            return ipaddress.ip_address(self.address_v6).packed

    def address_v4_ipaddress(self):
        """ Get packed address """
        if self.address_v4 is None:
            return None
        else:
            return ipaddress.ip_address(self.address_v4)


    def address_v6_ipaddress(self):
        """ Get packed address """
        if self.address_v6 is None:
            return None
        else:
            return ipaddress.ip_address(self.address_v6)

    def to_tuple(self):
        """ Get the message as tuple to send to the network """
        return (
            self.identity,
            self.data,
            self.encoding,
            self.address_v6_packed(),
            self.address_v4_packed(),
            self.status,
            self.port,
        )
