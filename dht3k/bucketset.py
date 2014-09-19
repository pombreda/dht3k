import heapq
import threading
import collections

from .peer    import Peer
from .hashing import bytes2int


def largest_differing_bit(value1, value2):
    int1 = bytes2int(value1)
    int2 = bytes2int(value2)
    distance = int1 ^ int2
    length = -1
    while (distance):
        distance >>= 1
        length += 1
    return max(0, length)


class BucketSet(object):
    def __init__(self, bucket_size, buckets, id_):
        self.id = id_
        self.bucket_size = bucket_size
        self.buckets = [collections.OrderedDict() for _ in range(buckets)]
        self.lock = threading.Lock()

    def insert(self, peer):
        assert isinstance(peer, Peer)
        if peer.id != self.id:
            bucket_number = largest_differing_bit(self.id, peer.id)
            peer_tuple = peer.astuple()
            with self.lock:
                bucket = self.buckets[bucket_number]
                old_peer = None
                try:
                    old_peer = Peer(*bucket[peer.id])
                except KeyError:
                    pass
                if old_peer:
                    del bucket[peer.id]
                    if not peer.hostv4:
                        peer.hostv4 = old_peer.hostv4
                    if not peer.hostv6:
                        peer.hostv6 = old_peer.hostv6
                elif len(bucket) >= self.bucket_size:
                    bucket.popitem(0)
                bucket[peer.id] = peer_tuple

    def peers(self):
        return (peer for bucket in self.buckets for peer in bucket.values())

    def nearest_nodes(self, key, limit=None):
        num_results = limit if limit else self.bucket_size
        with self.lock:
            def keyfunction(peer):
                ikey  = bytes2int(key)
                ipeer = bytes2int(peer[1])
                return ikey ^ ipeer
            peers = self.peers()
            best_peers = heapq.nsmallest(num_results, peers, keyfunction)
            return [Peer(*peer) for peer in best_peers]
