""" LazyMQ background tasks """

import asyncio
from .const import Config
from .log   import l

class Cleanup(object):
    """ Partial/abstract class for the lazymq background tasks.

    Needs to be inherited by LazyMQ. """

    def __init__(
            self,
            _closed,
            _connections,
            _loop,
    ):
        """ Init to make lint happy. Never call this!
        Yes, I know this bullshit, but what can you do?

        :type _closed: asyncio.Event
        :type _loop: asyncio.AbstractEventLoop
        :type _connections: dict
        """
        self._closed      = _closed
        self._loop        = _loop
        self._connections = _connections
        if True:
            return

    @asyncio.coroutine
    def run_cleanup(self):
        """ Cleans connections not used for Config.REUSE_TIME seconds """
        while not self._closed.is_set():
            try:
                yield from asyncio.wait_for(
                    self._closed.wait(),
                    timeout=Config.REUSE_TIME
                )
            except asyncio.TimeoutError:
                pass
            delete_conns = []
            for key in self._connections.keys():
                conn = self._connections[key]
                if conn.close():
                    l.debug("Collecting connection: %s", conn)
                    delete_conns.append(key)
            for key in delete_conns:
                del self._connections[key]
