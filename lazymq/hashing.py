import hashlib
import os

from .const import Config


def bytes2int(data):
    return int.from_bytes(data, 'big')  # Network oder (important)

def int2bytes(value):
    return value.to_bytes(Config.HASH_BYTES, 'big')  # Network oder (important)

def hash_function(data):
    s = hashlib.sha256()
    s.update(data)
    return s.digest()

def random_id(seed=None):
    return os.urandom(Config.HASH_BYTES)


