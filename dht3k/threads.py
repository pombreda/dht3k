""" Anything to do with threading and background maintainance """
from concurrent.futures import ThreadPoolExecutor

from .const import Config
from .log   import l

pool = ThreadPoolExecutor(max_workers=Config.WORKERS)


class ThreadPoolMixIn:
    """Mix-in class to handle each request in a new thread."""

    # Decides how threads will act upon termination of the
    # main process
    daemon_threads = False

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.

        In addition, exception handling is done here.

        """
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except:  # noqa
            l.exception("Exception in request handler")
            self.handle_error(request, client_address)
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        """Submit a new job to process the request."""
        pool.submit(self.process_request_thread, request, client_address)
