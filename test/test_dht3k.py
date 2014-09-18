"""
Integration tests for pydht
"""

from dht3k import DHT
import pytest
import time


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
        self.dht1.server.shutdown()
        self.dht2.server.shutdown()
        self.dht1.server.server_close()
        self.dht2.server.server_close()

    def test_find_set(self):
        """ Testing init """
        self.dht1[b"huhu"] = b"haha"
        time.sleep(0.2)
        assert self.dht2[b"huhu"] == b"haha"

    def test_perform(self):
        """ Testing init """
        for x in range(30):
            x += 1
            self.dht1[x] = x
        time.sleep(1)
        for x in range(30):
            x += 1
            assert self.dht2[x] == x
        assert False

    def test_find_set_str(self):
        """ Testing init """
        self.dht1["huhu"] = b"haha"
        time.sleep(0.2)
        assert self.dht2["huhu"] == b"haha"

    def test_not_find(self):
        """ Testing init """
        self.dht1[b"huhu"] = b"haha"
        time.sleep(0.2)
        with pytest.raises(KeyError):
            assert self.dht2[b"blau"] == b"haha"

    def test_null_key(self):
        """ Testing init """
        self.dht1[0] = b"haha"
        time.sleep(0.2)
        assert self.dht2[0] == b"haha"

# pylama:ignore=w0201
