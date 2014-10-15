#pylint: disable=no-self-use
"""
Testing the lazymq module
"""

try:
    import unittest.mock   as mock
except ImportError:
    import mock

import lazymq
import asyncio

class TestLazyMQ(object):
    """ Testing the bucketset """

    def setup(self):
        """ Setup """

    def teardown(self):
        """ Teardown """

    def test_main(self):
        """ Test sending and receiving one message """
        mqa = lazymq.LazyMQ(port=4320)
        mqb = lazymq.LazyMQ(port=4321)
        mqa.start()
        mqb.start()
        asyncio.get_event_loop().run_forever()
