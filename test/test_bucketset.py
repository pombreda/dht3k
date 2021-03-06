
"""
Testing the bucketset
"""

try:
    import unittest.mock   as mock
except ImportError:
    import mock

import dht3k.bucketset     as bucketset
import dht3k.peer          as peer
import dht3k.helper        as helper


class TestBucketset(object):
    """ Testing the bucketset """

    def setup(self):
        """ Setup """

    def teardown(self):
        """ Teardown """

    def test_main(self):
        """ Testing basic bucket operation """
        bs = bucketset.BucketSet(4, 32, b"aaaa")
        server = mock.Mock()
        server.dht = mock.Mock()
        server.dht.rpc_states = helper.LockedDict()
        server.dht.peer = mock.Mock()
        server.dht.peer.id = b"aaaa"
        with mock.patch.object(
            peer.Peer, 'ping', return_value=None
        ) as mock_ping:
            bs.insert(
                peer.Peer(2, b"caaa"),
                server
            )
            bs.insert(
                peer.Peer(2, b"caab"),
                server
            )
            bs.insert(
                peer.Peer(2, b"caac"),
                server
            )
            bs.insert(
                peer.Peer(2, b"caad"),
                server
            )
            bs.insert(
                peer.Peer(2, b"caae"),
                server
            )
            bs.insert(
                peer.Peer(2, b"caaa"),
                server,
                True
            )
        # TODO: Check for correct ping
        res = (
            {b'caab': (2,
                       b'caab',
                       None,
                       None,
                       False),
             b'caaa': (2,
                       b'caaa',
                       None,
                       None,
                       True),
             b'caac': (2,
                       b'caac',
                       None,
                       None,
                       False),
             b'caad': (2,
                       b'caad',
                       None,
                       None,
                       False)}
        )
        assert bs.buckets[25] == res
        assert mock_ping.called
        assert mock_ping.call_args[0][1] == b"aaaa"
