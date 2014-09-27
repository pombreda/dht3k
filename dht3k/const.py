""" Constants for pydht """


class Config(object):
    """ Config constants """
    K          = 20
    ALPHA      = 3
    ID_BYTES   = 32
    ID_BITS    = ID_BYTES * 8
    SLEEP_WAIT = 1


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
    RPC_ID        = 11
    NEAREST_NODES = 12
    ALL_ADDR      = 13
    CLI_ADDR      = 14
