import hashlib
import os

id_bytes = 32


def hash_function(data):
    s = hashlib.sha256()
    s.update(data)
    return s.digest()


def random_id(seed=None):
    return os.urandom(id_bytes)
