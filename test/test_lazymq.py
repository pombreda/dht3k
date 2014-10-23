#pylint: disable=no-self-use,broad-except
#pylint: disable=attribute-defined-outside-init
"""
Testing the lazymq module
"""

try:
    import unittest.mock   as mock
except ImportError:
    import mock

import lazymq
import asyncio
import pytest
import ipaddress
from lazymq.log import l

class TestLazyMQ(object):
    """ Testing the bucketset """

    def setup(self):
        """ Setup """
        asyncio.get_event_loop().set_debug(True)
        lazymq.log.log_to_stderr(True)
        self.mqa = lazymq.LazyMQ(port=4320)
        self.mqb = lazymq.LazyMQ(port=4321)
        self.mqa.loop.run_until_complete(self.mqa.start())
        self.mqb.loop.run_until_complete(self.mqb.start())

    def teardown(self):
        """ Teardown """
        self.mqa.loop.run_until_complete(self.mqa.close())
        self.mqb.loop.run_until_complete(self.mqb.close())


    def test_main(self):
        """ Test sending and receiving one message """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            msg = lazymq.Message(
                data = b"hello",
                address_v4 = "127.0.0.1",
                port=4321
            )
            yield from self.mqa.deliver(msg)
        asyncio.async(run())
        res = self.mqa.loop.run_until_complete(self.mqb.receive())
        assert res.data == b"hello"

    def test_second(self):
        """ Test sending and receiving one message """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            msg = lazymq.Message(
                data = b"hello",
                address_v4 = "127.0.0.1",
                port=4321
            )
            yield from self.mqa.deliver(msg)
        asyncio.async(run())
        res = self.mqa.loop.run_until_complete(self.mqb.receive())
        assert res.data == b"hello"

    def test_fuzz(self):
        """ Test sending and receiving one message """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            conn = (yield from self.mqa.get_connection(
                port = 4321,
                address_v4 = ipaddress.ip_address("127.0.0.1")
            ))
            assert isinstance(conn, lazymq.struct.Connection)
            with (yield from conn) as (_, writer):
                writer.write(lazymq.hashing.random_id())
            conn.close()
            self.mqa._connections.clear()
            msg = lazymq.Message(
                data = b"hello",
                address_v4 = "127.0.0.1",
                port=4321
            )
            yield from self.mqa.deliver(msg)
        asyncio.async(run())
        res = self.mqa.loop.run_until_complete(self.mqb.receive())
        assert res.data == b"hello"

    def test_simple_load_test(self):
        """ Test load test using two senders one receiver """
        msgs1 = set(range(100))
        msgs2 = list(msgs1)
        @asyncio.coroutine
        def send(mq):
            """ Testrunner """
            l.debug("sending simple load test msgs")
            while msgs2:
                num = msgs2.pop()
                l.debug("Sending message: %d", num)
                msg = lazymq.Message()
                msg.port = 4320
                msg.address_v6 = "::1"
                msg.data = num
                yield from mq.deliver(msg)

        @asyncio.coroutine
        def receive():
            """ Testrunner """
            yield from asyncio.sleep(0.1)
            while msgs1:
                msg = yield from self.mqa.receive()
                l.debug("Receiving message: %d", msg.data)
                try:
                    msgs1.remove(msg.data)
                except KeyError:
                    pass
        try:
            self.mqc = lazymq.LazyMQ(port=4322)
            self.mqc.loop.run_until_complete(self.mqc.start())
            asyncio.async(send(self.mqb))
            asyncio.async(send(self.mqc))
            self.mqa.loop.run_until_complete(receive())
        finally:
            self.mqc.loop.run_until_complete(self.mqc.close())

    def test_ping(self):
        """ Test ping """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            msg = lazymq.Message(
                status = lazymq.const.Status.PING,
                address_v4 = "127.0.0.1",
                port=4321
            )
            return (yield from self.mqa.communicate(msg))
        res = self.mqa.loop.run_until_complete(run())
        assert isinstance(res, lazymq.Message)
        assert res.status == lazymq.const.Status.PONG

    def test_connection_refused(self):
        """ Test if we get connection refused """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            return (yield from self.mqa.get_connection(
                port = 12412,
                address_v4 = ipaddress.ip_address("127.0.0.1")
            ))
        with pytest.raises(OSError):
            self.mqa.loop.run_until_complete(run())

    def test_connection(self):
        """ Test if we get connection refused """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            return (yield from self.mqa.get_connection(
                port = 4321,
                address_v4 = ipaddress.ip_address("127.0.0.1")
            ))
        self.mqa.loop.run_until_complete(run())
