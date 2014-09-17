"""
Integration tests for pydht
"""

from dht3k import DHT


class TestPyDht(object):
    """ Testing the DHT """

    def setup(self):
        """ Setup """
        self.dht1 = DHT("localhost", 4165)
        self.dht2 = DHT(
            "localhost",
            4166,
            boot_host="localhost",
            boot_port=4165
        )

    def teardown(self):
        """ Teardown """
        pass

    def test_init(self):
        """ Testing init """
        self.dht1[b"huhu"] = b"haha"
        assert self.dht2[b"huhu"] == b"haha"
# pylama:ignore=w0201
