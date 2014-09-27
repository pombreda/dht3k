import heapq
import threading
import collections
import time

from .peer    import Peer
from .hashing import bytes2int, random_id


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

    def insert(self, peer, server, from_pong=False):
        assert isinstance(peer, Peer)
        if peer.id != self.id:
            bucket_number = largest_differing_bit(self.id, peer.id)
            peer_tuple = peer.astuple()
            with self.lock:
                bucket = self.buckets[bucket_number]
                old_peer = None
                try:
                    old_peer = Peer(
                        *bucket[peer.id],
                        is_bytes=True
                    )
                except KeyError:
                    pass
                if old_peer:
                    del bucket[peer.id]
                    if not peer.hostv4:
                        peer.hostv4 = old_peer.hostv4
                    if not peer.hostv6:
                        peer.hostv6 = old_peer.hostv6
                    bucket[peer.id] = peer.astuple()
                elif len(bucket) >= self.bucket_size:
                    if from_pong:
                        bucket.popitem(-1)
                        items = list(bucket.items())
                        # Putting the pinged node in the 3/4 (%75) of the
                        # bucket is a simple async emultion of the protocol
                        # defined by kademlia, I hope it works ;-)
                        items.insert(
                            int(self.bucket_size * 0.75),
                            (
                                peer.id,
                                peer_tuple
                            )
                        )
                        bucket = collections.OrderedDict(items)
                        self.buckets[bucket_number] = bucket
                        print("from pong")
                    else:
                        pop_peer = Peer(
                            *bucket.popitem(0)[1],
                            is_bytes=True
                        )
                        rpc_id = random_id()
                        with server.dht.rpc_states as states:
                            states[rpc_id] = [time.time()]
                        pop_peer.ping(
                            server.dht,
                            server.dht.peer.id,
                            rpc_id
                        )
                        bucket[peer.id] = peer_tuple
                else:
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
            return [Peer(
                *peer,
                is_bytes=True
            ) for peer in best_peers]
