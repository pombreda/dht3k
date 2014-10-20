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

class TestLazyMQ(object):
    """ Testing the bucketset """

    def setup(self):
        """ Setup """
        lazymq.log.log_to_stderr(True)
        self.mqa = lazymq.LazyMQ(port=4320)
        self.mqb = lazymq.LazyMQ(port=4321)
        self.mqa.start()
        self.mqb.start()

    def teardown(self):
        """ Teardown """
        self.mqa.close()
        self.mqb.close()


    #def test_main(self):
    #    """ Test sending and receiving one message """
    #    @asyncio.coroutine
    #    def run():
    #        """ Testrunner """
    #        yield from asyncio.sleep(0.1)
    #        msg = lazymq.Message(
    #            data = b"hello",
    #            address_v4 = b"127.0.0.1",
    #            port=4321
    #        )
    #        yield from self.mqa.deliver(msg)
    #    asyncio.async(run())
    #    res = self.mqa.loop.run_until_complete(self.mqb.receive())
    #    assert res.data == b"hello"

    #def test_second(self):
    #    """ Test sending and receiving one message """
    #    @asyncio.coroutine
    #    def run():
    #        """ Testrunner """
    #        yield from asyncio.sleep(0.1)
    #        msg = lazymq.Message(
    #            data = b"hello",
    #            address_v4 = b"127.0.0.1",
    #            port=4321
    #        )
    #        yield from self.mqa.deliver(msg)
    #    asyncio.async(run())
    #    res = self.mqa.loop.run_until_complete(self.mqb.receive())
    #    assert res.data == b"hello"

    def test_multi_receive(self):
        """ Test sending and receiving one message """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            yield from asyncio.sleep(1)
            print("do")
            msg = lazymq.Message(
                data = b"hello",
                address_v4 = b"127.0.0.1",
                port=4321
            )
            yield from self.mqa.deliver(msg)
        asyncio.async(run())
        done, _ = self.mqa.loop.run_until_complete(
            asyncio.wait([
                self.mqb.receive(),
                self.mqb.receive(),
            ])
        )
        for msg in done:
            assert msg.result().data == b"hello"

    def test_connection_refused(self):
        """ Test if we get connection refused """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            return (yield from self.mqa.get_connection(
                port = 12412,
                address_v4 = "127.0.0.1"
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
                address_v4 = "127.0.0.1"
            ))
        self.mqa.loop.run_until_complete(run())
