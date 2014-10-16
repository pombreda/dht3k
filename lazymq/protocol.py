""" Defines the lazymq protocol """

import asyncio
import msgpack

from .struct import Message, Connection
from .hashing import random_id

class Protocol(object):
    """ Partial/abstract class for the lazymq protocol.

    Needs to be inherited by LazyMQ. """

    def __init__(self):
        """ Init to make lint happy """
        self.encoding     = None
        self.port         = None
        self._connections = None

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

    @asyncio.coroutine
    def _handle_connection(self, reader, writer):
        """ Handles a connection

        :type reader: asyncio.StreamReader
        :type writer: asyncio.StreamWriter """

        peer = writer.get_extra_info('peername')
        conn = Connection(reader, writer)
        self._connections[peer] = conn
        print("set", peer)
        while True:
            done, pending = yield from asyncio.wait([
                reader.readexactly(1),
                conn.writing.wait(),
            ], return_when=asyncio.FIRST_COMPLETED)
            for pen in pending:
                pen.cancel()
            done = list(done)
            if isinstance(done[0].exception(), asyncio.IncompleteReadError):
                # Connection was closed
                conn.close()
                # TODO: here late closes can occour after we actually already
                # cleaned up. I think it is ok to just ignore these. But lets
                # think about it later again.
                try:
                    del self._connections[peer]
                except KeyError:
                    pass
                print("del", peer)
                return
            import ipdb; ipdb.set_trace()



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
        with (yield from conn) as (reader, writer):
            writer.write(enclenb)
            writer.write(enc)
            writer.write(msglenb)
            writer.write(msg)
            # status = yield from asyncio.wait_for(
            #     reader.readexactly(1),
            #     const.Config.TIMEOUT,
            # )
            # TODO: status as exception
