"""
Testing hashing fuctions
"""

import dht3k.hashing as hashing


class TestHashing(object):
    """ Testing the hashing """

    def setup(self):
        """ Setup """

    def teardown(self):
        """ Teardown """

    def test_bytes2int(self):
        """ Test bytes conv functions and python 2 compat """
        val = 3234
        res_val = hashing.bytes2int(
            hashing.int2bytes(val)
        )
        assert val == res_val
        bytes_ = b"1234567890" * 3
        bytes_ += b"12"
        res_bytes = hashing.int2bytes(
            hashing.bytes2int(bytes_)
        )
        assert bytes_ == res_bytes

        assert hashing.bytes2int(b"huhu") == 1752524917
        assert hashing.bytes2int(
            b"huhu lasdjflakjsdlfa"
        ) == 596353326887779266705144559898163325336086668897
        assert hashing.int2bytes(
            38094125654575597149245727851023
        ) == b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xe0\xd0\xc21\x91U"DA@\xdcB\x0f'
