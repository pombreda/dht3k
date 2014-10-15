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
        self.mqa = lazymq.LazyMQ(port=4320)
        self.mqb = lazymq.LazyMQ(port=4321)

    def teardown(self):
        """ Teardown """
        self.mqa.close()
        self.mqb.close()


    def test_main(self):
        """ Test sending and receiving one message """
        @asyncio.coroutine
        def run():
            """ Testrunner """
            msg = lazymq.Message(
                data = b"hello",
                address_v4 = b"127.0.0.1"
            )
            yield from self.mqa.deliver(msg)
        self.mqa.loop.run_until_complete(run())

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
