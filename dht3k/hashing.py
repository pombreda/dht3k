import hashlib
import os
import six

id_bytes = 32

if six.PY3:
    def bytes2int(data):
        return int.from_bytes(data, 'big')  # Network oder (important)
else:
    def bytes2int(data):
        return int(data.encode('hex'), 16)

def hash_function(data):
    s = hashlib.sha256()
    s.update(data)
    return s.digest()


def random_id(seed=None):
    return os.urandom(id_bytes)
