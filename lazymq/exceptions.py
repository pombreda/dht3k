""" Module containing custom exceptions """

class BadMessage(Exception):
    """ Raised when a message can't be parsed or a timeout occurs """
    pass

class ClosedException(Exception):
    """ Raised when lazymq closes a connection and stops the handle loop """
