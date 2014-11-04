""" Constants for pydht """
import lazymq.const as lazyconst


def _consts_to_dict(object_):
    """Converts a constants object to a dictionary"""
    new = {}
    for const in dir(object_):
        if not const.startswith("_"):
            new[getattr(object_, const)] = const
    return new


class Config(object):
    """ Config constants """
    K              = 20
    ALPHA          = 3
    FW_PENALTY     = 2 ** (lazyconst.Config.HASH_BITS + 1)
    SLEEP_WAIT     = 1
    ID_BYTES       = lazyconst.Config.HASH_BYTES
    ID_BITS        = lazyconst.Config.HASH_BITS
    BUCKET_REFRESH = 1200  # NATs should all be timeouted after that time!
    FIREWALL_CHECK = 3600
    PORT           = 7339
    RPC_TIMEOUT    = 30
    WORKERS        = 40
    NETWORK_ID     = (
        b'\xc4\x82{\x0e\xf3\x99\x9f\x10.m=\x12\xef3\x19['
        b'Q\xac\x14G\xc9\x8ft\xb5\xb2z\xb6\x84\x91$\xac\x03'
    )


config_dict = _consts_to_dict(Config)


class MinMax(object):
    """ Maximima and minima """
    MAX_IP_LEN     = 16
    MIN_IP_LEN     = 4
    PEER_TUPLE_LEN = 4

min_max_dict = _consts_to_dict(MinMax)


class Message(object):
    """ Constants to use in message encoding """
    PING          = 0
    PONG          = 1
    FIND_NODE     = 2
    FIND_VALUE    = 3
    FOUND_NODES   = 4
    FOUND_VALUE   = 5
    STORE         = 6
    PEER_ID       = 7
    ID            = 8
    MESSAGE_TYPE  = 9
    VALUE         = 10
    NEAREST_NODES = 12
    ALL_ADDR      = 13
    CLI_ADDR      = 14

message_dict = _consts_to_dict(Message)


class Storage(object):
    """ Storage type """
    NONE   = None
    MEMORY = "memory"
    DISK   = "disk"
