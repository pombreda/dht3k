""" Anything to do with threading and background maintainance """
from concurrent.futures import ThreadPoolExecutor
import threading

from .const   import Config
from .log     import l
from .hashing import int2bytes

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


def run_check_firewalled(dht):
    """ Refresh the buckets by finding nodes near that bucket """

    def task():
        """ Run the task """
        try:
            dht.stop.wait(Config.SLEEP_WAIT)
            while dht.firewalled:
                dht.boot_peer.fw_ping(dht, dht.peer.id)
                l.info("Executed firewall check")
                if dht.stop.wait(Config.FIREWALL_CHECK):
                    return
        except:  # noqa
            l.exception("run_check_firewalled failed")
            raise
        finally:
            l.info("run_check_firewalled ended")

    t = threading.Thread(target=task)
    t.setDaemon(True)
    t.start()
    return t

def run_bucket_refresh(dht):  # noqa
    """ Refresh the buckets by finding nodes near that bucket """

    def refresh_bucket(x):
        """ Refresh a single bucket """
        id_ = int2bytes(2 ** x)
        dht.iterative_find_nodes(id_)

    def task():
        """ Run the task """
        try:
            if dht.boot_peer:
                for x in range(Config.ID_BITS):
                    refresh_bucket(x)
                    l.debug("Refreshed bucket %d", x)
                    if dht.stop.wait(Config.SLEEP_WAIT * 20):
                        return
            while True:
                for x in range(Config.ID_BITS):
                    refresh_bucket(x)
                    l.info("Refreshed bucket %d", x)
                    if dht.firewalled:
                        f = 20
                    else:
                        f = 1
                    if dht.stop.wait(Config.BUCKET_REFRESH * f):
                        return
        except:  # noqa
            l.exception("run_bucket_refresh failed")
            raise
        finally:
            l.info("run_bucket_refresh ended")

    t = threading.Thread(target=task)
    t.setDaemon(True)
    t.start()
    return t
