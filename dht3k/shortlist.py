import threading
import concurrent.futures as futures
import collections

from .peer    import Peer
from .hashing import bytes2int
from .const   import Config


class Shortlist(object):

    def __init__(self, k, key, my_id):
        self.k                = k
        self.key              = key
        self.my_id            = my_id
        self.list             = list()
        self.lock             = threading.Lock()
        self.completion_value = futures.Future()
        self.completion_value.set_running_or_notify_cancel()
        self.updated          = threading.Condition()

    def set_complete(self, value):
        with self.updated:
            self.updated.notify_all()
        self.completion_value.set_result(value)

    def completion_result(self):
        try:
            return self.completion_value.result(Config.SLEEP_WAIT)
        except futures.TimeoutError:
            raise KeyError("Not found due network timeout")

    def update(self, nodes):
        for node in nodes:
            self._update_one(node)
        with self.updated as up:
            self.updated.notify_all()

    def _update_one(self, node):
        if (
                node.id == self.key or
                node.id == self.my_id or
                self.completion_value.done()
        ):
            return
        with self.lock:
            for i in range(len(self.list)):
                if node.id == self.list[i][0][1]:
                    break
                iid   = bytes2int(node.id)
                ikey  = bytes2int(self.key)
                ilist = bytes2int(self.list[i][0][1])
                if iid ^ ikey < ilist ^ ikey:
                    self.list.insert(i, (node.astuple(), False))
                    self.list = self.list[:self.k]
                    break
            else:
                # Executed if we hit no break above which means
                # 1. The new node isn't duplicated
                # 2. The new node is not nearer than any other nodes
                if len(self.list) < self.k:
                    self.list.append((node.astuple(), False))

    def mark(self, node):
        with self.lock:
            for i in range(len(self.list)):
                if node.id == self.list[i][0][1]:
                    self.list[i] = (node.astuple(), True)

    def complete(self):
        if self.completion_value.done():
            return True
        with self.lock:
            for node, completed in self.list:
                if not completed:
                    return False
            return True

    def get_next_iteration(self, alpha):
        if self.completion_value.done():
            return []
        next_iteration = []
        with self.lock:
            for node, completed in self.list:
                if not completed:
                    next_iteration.append(Peer(*node, is_bytes=True))
                    if len(next_iteration) >= alpha:
                        break
        return next_iteration

    def results(self):
        with self.lock:
            return [Peer(
                *node,
                is_bytes=True
            ) for (node, completed) in self.list]
