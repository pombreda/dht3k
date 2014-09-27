import hashlib
import os
import six

from .const import Config


if six.PY3:  # pragma: no cover
    def bytes2int(data):
        return int.from_bytes(data, 'big')  # Network oder (important)

    def int2bytes(value):
        return value.to_bytes(Config.ID_BYTES, 'big')  # Network oder (important)
else:  # pragma: no cover
    def bytes2int(str):
        return int(str.encode('hex'), 16)

    def bytes2hex(str):
        return '0x'+str.encode('hex')

    def int2bytes(i):
        h = int2hex(i)
        return hex2bytes(h)

    def int2hex(i):
        h = hex(i)
        if h[-1] == "L":
            h = h[:-1]
        return h

    def hex2int(h):
        if len(h) > 1 and h[0:2] == '0x':
            h = h[2:]

        if len(h) % 2:
            h = "0" + h

        return int(h, 16)

    def hex2bytes(h):
        if len(h) > 1 and h[0:2] == '0x':
            h = h[2:]

        add = Config.ID_BYTES * 2 - len(h)
        h = "0" * add + h

        return h.decode('hex')

def hash_function(data):
    s = hashlib.sha256()
    s.update(data)
    return s.digest()


def random_id(seed=None):
    return os.urandom(Config.ID_BYTES)
