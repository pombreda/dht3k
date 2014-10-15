# pylint: disable=wildcard-import,unused-wildcard-import
""" Hashing has been moved to lazymq """
from .const         import Config
from lazymq.hashing import *

def rpc_to_hash_id(rpc_id):
    return hash_function(rpc_id + Config.NETWORK_ID)


def rpc_id_pair(seed=None):
    rpc_id = random_id()
    return (rpc_id, hash_function(rpc_id + Config.NETWORK_ID))
