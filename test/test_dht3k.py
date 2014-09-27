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
        time.sleep(0.4)
        self.dht1 = DHT(
            4165,
            u"127.0.0.1",
            u"::1",
            listen_hostv4 = u"127.0.0.1",
            listen_hostv6 = u"::1",
            port_map      = False,
        )
        self.dht2 = DHT(
            4166,
            u"127.0.0.1",
            u"::1",
            listen_hostv4 = u"127.0.0.1",
            listen_hostv6 = u"::1",
            boot_host     = u"::1",
            boot_port     = 4165,
            port_map      = False,
        )

    def teardown(self):
        """ Teardown """
        self.dht1.close()
        self.dht2.close()
        time.sleep(0.4)

    def test_find_set(self):
        """ Testing init """
        self.dht1[b"huhu"] = b"haha"
        time.sleep(0.1)
        assert self.dht2[b"huhu"] == b"haha"

    def test_encoding(self):
        """ Testing init """
        self.dht1.encoding = "UTF-8"
        self.dht2.encoding = "UTF-8"
        self.dht1["huhu"] = "haha"
        time.sleep(0.1)
        assert self.dht2["huhu"] == "haha"

    def test_perform(self):
        """ Testing init """
        for x in range(10):
            x += 1
            self.dht1[x] = x
        time.sleep(1)
        for x in range(10):
            x += 1
            assert self.dht2[x] == x

    def test_find_set_str(self):
        """ Testing init """
        self.dht1["huhu"] = b"haha"
        time.sleep(0.1)
        assert self.dht2["huhu"] == b"haha"

    def test_not_find(self):
        """ Testing init """
        self.dht1[b"huhu"] = b"haha"
        time.sleep(0.1)
        with pytest.raises(KeyError):
            assert self.dht2[b"blau"] == b"haha"

    def test_null_key(self):
        """ Testing init """
        self.dht1[0] = b"haha"
        time.sleep(0.1)
        assert self.dht2[0] == b"haha"

    def test_null_value(self):
        """ Testing init """
        self.dht1[b"bla"] = 0
        time.sleep(0.1)
        assert self.dht2["bla"] == 0

    @pytest.mark.slowtest
    def test_large_network(self):
        """ Testing a larger network """
        dhts = []
        try:
            for x in range(100):
                try:
                    dhts.append(DHT(
                        x + 30000,
                        u"127.0.0.1",
                        u"::1",
                        listen_hostv4 = u"127.0.0.1",
                        listen_hostv6 = u"::1",
                        boot_host     = u"::1",
                        boot_port     = 4165,
                        port_map      = False,
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
                print([len(a) for a in dht.buckets.buckets])
                dht.close()

# pylama:ignore=w0201
