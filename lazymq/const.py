# pylint: disable=too-few-public-methods
# pylint: disable=invalid-name
""" Constants for lazymq """


def _consts_to_dict(object_):
    """Converts a constants object to a dictionary"""
    new = {}
    for const in dir(object_):
        if not const.startswith("_"):
            new[getattr(object_, const)] = const
    return new

class Protocols(object):
    """ Protocol selection """
    IPV4 = 2 ** 0
    IPV6 = 2 ** 1

class Config(object):
    """ Config constants """
    PORT       = 7339
    ENCODING   = None
    HASH_BYTES = 32
    HASH_BITS  = HASH_BYTES * 8
    PROTOS     = Protocols.IPV4 | Protocols.IPV6
    BACKLOG    = 100
    TIMEOUT    = 5
    REUSE_TIME = 30


config_dict = _consts_to_dict(Config)

class Status(object):
    """ Message status """
    SUCCESS              = 0
    HOST_NOT_REACHABLE   = 1
    CONNECTION_REFUSED   = 2
    TIMEOUT              = 3
    MESSAGE_NOT_ACCEPTED = 4

status_dict = _consts_to_dict(Status)
