""" Handling the bucketset """
import heapq
import threading
import time
import binascii
import asyncio

from .peer    import Peer
from .hashing import bytes2int, rpc_id_pair
from .log     import l
from .const   import Config


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
        self.buckets = [[] for _ in range(buckets)]
        self.lock = threading.Lock()

    @asyncio.coroutine
    def insert(self, peer, server):
        assert isinstance(peer, Peer)
        if peer.id != self.id:
            bucket_number = largest_differing_bit(self.id, peer.id)
            peer_tuple = peer.astuple()
            with self.lock:
                bucket = self.buckets[bucket_number]
                old_peer = None
                try:
                    old_peer = Peer(
                        *bucket[peer.id]
                    )
                except KeyError:
                    pass
                if old_peer:
                    del bucket[peer.id]
                    if not peer.host_v4:
                        peer.host_v4 = old_peer.host_v4
                    if not peer.host_v6:
                        peer.host_v6 = old_peer.host_v6
                    bucket[peer.id] = peer.astuple()
                elif len(bucket) >= self.bucket_size:
                    pop_peer = Peer(*bucket[0])
                    try:
                        yield from pop_peer.ping(
                            server.dht,
                        )
                    except (asyncio.TimeoutError, OSError):
                        del bucket[0]
                        bucket.append(peer_tuple)
                    except Exception:
                        l.exception("Unhandled exception: please fix")
                        raise
                else:
                    bucket[peer.id] = peer_tuple

    def peers(self):
        return (peer for bucket in self.buckets for peer in bucket.values())

    def peerslist(self):
        """ Returns a complete list of peers """
        with self.lock:
            return list(self.peers())

    def nearest_nodes(self, key, limit=None):
        num_results = limit if limit else self.bucket_size
        with self.lock:
            def keyfunction(peer):
                ikey  = bytes2int(key)
                ipeer = bytes2int(peer[1])
                if peer[4]:
                    penalty = 0
                else:
                    # This peer is probably firewalled, return it after
                    # well connected peers
                    penalty = Config.FW_PENALTY
                return (ikey ^ ipeer) + penalty
            peers = self.peers()
            # When sorting well connected nodes are returned first
            best_peers = heapq.nsmallest(num_results, peers, keyfunction)
            return [Peer(
                *peer
            ) for peer in best_peers]
