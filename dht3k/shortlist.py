import threading
import concurrent.futures as futures

from .peer    import Peer
from .hashing import bytes2int


class Shortlist(object):
    iteration_sleep = 1

    def __init__(self, k, key, my_id):
        self.k                = k
        self.key              = key
        self.my_id            = my_id
        self.list             = list()
        self.lock             = threading.Lock()
        self.completion_value = futures.Future()
        self.completion_value.set_running_or_notify_cancel()
        self.updated          = threading.Event()

    def set_complete(self, value):
        self.updated.set()
        self.completion_value.set_result(value)

    def completion_result(self):
        try:
            return self.completion_value.result(Shortlist.iteration_sleep)
        except futures.TimeoutError:
            raise KeyError("Not found due network timeout")

    def update(self, nodes):
        for node in nodes:
            self._update_one(node)
        self.updated.set()

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
                    next_iteration.append(Peer(*node))
                    if len(next_iteration) >= alpha:
                        break
        return next_iteration

    def results(self):
        with self.lock:
            return [Peer(*node) for (node, completed) in self.list]
