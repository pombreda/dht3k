""" Helpers """

import six

if six.PY3:
    def sixunicode(data, is_bytes=False):
        return data
else:
    def sixunicode(data, is_bytes=False):
        if is_bytes:
            return data
        return unicode(data)
