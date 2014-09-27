""" Module for custom Exceptions """


class MaxSizeException(Exception):
    """ Maximum size of something is reached """
    pass


class NetworkError(Exception):
    """ Network error """
    pass
