"""
Integration tests for pydht
"""

from dht3k import DHT
from dht3k import hashing
import pytest
import time
import random


class TestPyDht(object):
    """ Testing the DHT """

    def setup(self):
        """ Setup """
        self.dht1 = DHT(4165, "127.0.0.1", "::1")
        self.dht2 = DHT(
            4166,
            "127.0.0.1",
            "::1",
            boot_host="::1",
            boot_port=4165
        )

    def teardown(self):
        """ Teardown """
        self.dht1.server4.shutdown()
        self.dht2.server4.shutdown()
        self.dht1.server4.server_close()
        self.dht2.server4.server_close()
        self.dht1.server6.shutdown()
        self.dht2.server6.shutdown()
        self.dht1.server6.server_close()
        self.dht2.server6.server_close()

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

    def test_null_value(self):
        """ Testing init """
        self.dht1[b"bla"] = 0
        time.sleep(0.2)
        assert self.dht2["bla"] == 0

    def test_large_network(self):
        """ Testing a larger network """
        dhts = []
        try:
            for x in range(100):
                try:
                    dhts.append(DHT(
                        x + 30000,
                        "127.0.0.1",
                        "::1",
                        boot_host="::1",
                        boot_port=4165,
                    ))
                except DHT.NetworkError:
                    pass
            for _ in range(4):
                for dht in dhts:
                    dht.iterative_find_nodes(hashing.random_id())
            self.dht2["baum"] = b"ast"
            for dht in dhts:
                assert dht["baum"] == b"ast"
            for x in range(30):
                dht1 = random.choice(dhts)
                dht2 = random.choice(dhts)
                x += 1
                dht1[x] = x
                time.sleep(0.2)
                assert dht2[x] == x

        finally:
            for dht in dhts:
                dht.server4.shutdown()
                dht.server4.server_close()
                dht.server6.shutdown()
                dht.server6.server_close()

# pylama:ignore=w0201
