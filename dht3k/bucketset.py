import heapq
import threading

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
        self.buckets = [list() for _ in range(buckets)]
        self.lock = threading.Lock()

    def insert(self, peer):
        if peer.id != self.id:
            bucket_number = largest_differing_bit(self.id, peer.id)
            peer_tuple = peer.astuple()
            with self.lock:
                bucket = self.buckets[bucket_number]
                if peer_tuple in bucket:
                    bucket.pop(bucket.index(peer_tuple))
                elif len(bucket) >= self.bucket_size:
                    bucket.pop(0)
                bucket.append(peer_tuple)

    def peers(self):
        return (peer for bucket in self.buckets for peer in bucket)

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
