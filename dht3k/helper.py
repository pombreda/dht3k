""" Helpers """

import six
import threading

if six.PY3:
    def sixunicode(data, is_bytes=False):
        return data
else:
    def sixunicode(data, is_bytes=False):
        if is_bytes:
            return data
        return unicode(data)


class LockedDict(object):
    """ Dict with lock """
    def __init__(self):
        self.dict_ = {}
        self.lock = threading.RLock()

    def __enter__(self):
        """ Lock and get ids """
        self.lock.acquire()
        return self.dict_

    def __exit__(self, type_, value, traceback):
        """ Unlock after ids are used """
        self.lock.release()
