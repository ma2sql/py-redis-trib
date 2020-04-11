import unittest
from unittest.mock import Mock, MagicMock, patch

from redis_trib import (
    ClusterNode,
    AssertClusterError,
    AssertEmptyError,
    CannotConnectToRedis, 
)

import redis

def fake_redis(**attrs):
    mock = Mock()
    mock.info.return_value = {'cluster_enabled': 1}
    mock.cluster.return_value = {'cluster_known_nodes': 1}
    mock.configure_mock(**attrs)
    return mock

class TestClusterNode(unittest.TestCase):
    def setUp(self):
        self._addr = '192.168.56.101:7001'
        self._cluster_nodes = {
            "192.168.56.111:7000@17000": {
                "last_pong_rcvd": "1586276816567",
                "epoch": "123",
                "node_id": "475f54ec47b0272712b5e72dcb0c25143803cf66",
                "flags": "slave",
                "last_ping_sent": "0",
                "connected": True,
                "slots": [],
                "master_id": "9e8e145973797debec30dad723d7a66d07e3e6eb"
            },
            "192.168.56.112:7000@17000": {
                "last_pong_rcvd": "1586276815000",
                "epoch": "120",
                "node_id": "f56836b56b1ebc5d1fe2c2aeee93a86e662d8d2c",
                "flags": "myself,master",
                "last_ping_sent": "0",
                "connected": True,
                "slots": [
                    ["5461", "11264"],
                ],
                "master_id": "-"
            },
            "192.168.56.113:7000@17000": {
                "last_pong_rcvd": "1586276837000",
                "epoch": "121",
                "node_id": "d495eeac7879b00c354a963982e6fc6955a557f8",
                "flags": "master",
                "last_ping_sent": "0",
                "connected": True,
                "slots": [
                    ["0", "5460"],
                ],
                "master_id": "-"
            }
        }

    def testConnectRedis(self):
        with patch('redis.StrictRedis') as mock_redis:
            _r = fake_redis()
            mock_redis.return_value = _r
            node = ClusterNode(self._addr)
            node.connect()
            _r.ping.assert_called()

    def testConnectRedisAlreadyConnected(self):
        with patch('redis.StrictRedis') as mock_redis:
            _r = fake_redis()
            mock_redis.return_value = _r
            _r.ping.side_effect = redis.exceptions.ConnectionError()
            with self.assertRaises(CannotConnectToRedis):
                node = ClusterNode(self._addr)
                node.connect(abort=True)
   
    def testAssertCluster(self):
        node = ClusterNode(self._addr)
        with patch.object(node, '_r', fake_redis()):
            node.assert_cluster()

    def testAssertNotCluster(self):
        node = ClusterNode(self._addr)
        mock_config = {'info.return_value': {'cluster_enabled': 0}}
        with patch.object(node, '_r', fake_redis(**mock_config)):
            with self.assertRaises(AssertClusterError):
                node.assert_cluster()

    def testAssertEmpty(self):
        node = ClusterNode(self._addr)
        with patch.object(node, '_r', fake_redis()):
            node.assert_empty()
        
    def testAssertNotEmpty(self):
        node = ClusterNode(self._addr)
        mock_config = {'cluster.return_value': {'cluster_known_nodes': 0}}
        with patch.object(node, '_r', fake_redis(**mock_config)):
            with self.assertRaises(AssertEmptyError):
                node.assert_empty()

    def testLoadInfo(self):
        node = ClusterNode(self._addr)
        mock_config = {'cluster.return_value': self._cluster_nodes}
        with patch.object(node, '_r', fake_redis(**mock_config)):
            node.load_info()
            self.assertEqual(node._node_id, "f56836b56b1ebc5d1fe2c2aeee93a86e662d8d2c")
            self.assertListEqual(node._slots, list(range(5461, 11264+1)))

    def testClusterNodes(self):
        node = ClusterNode(self._addr)
        mock_config = {'cluster.return_value': self._cluster_nodes}
        with patch.object(node, '_r', fake_redis(**mock_config)):
            self.assertDictEqual(node.cluster_nodes, self._cluster_nodes)

    def testFriends(self):
        node = ClusterNode(self._addr)
        mock_config = {'cluster.return_value': self._cluster_nodes}
        with patch.object(node, '_r', fake_redis(**mock_config)):
            friends = [addr for addr, _ in node.friends]
            self.assertIn("192.168.56.111:7000@17000", friends)
            self.assertIn("192.168.56.113:7000@17000", friends)

    def testParseSlot(self):
        expected_slots = [
            ["0"],    
            ["1", "5460"],
            ["[5461", ">", "b35b55daf26e84acdf17fed30d46ca97ccdd9169]"],
            ["[5462", "<", "f24f7dd5c9ac963dbf086506f40f9b4fb3ecb2a1]"],
        ]
        node = ClusterNode(self._addr)
        slots, migrating, importing = node._parse_slots(expected_slots)
        self.assertListEqual(slots, list(range(0,5461)))
        self.assertDictEqual(migrating, {5461: "b35b55daf26e84acdf17fed30d46ca97ccdd9169"})
        self.assertDictEqual(importing, {5462: "f24f7dd5c9ac963dbf086506f40f9b4fb3ecb2a1"})

    def test_add_slots(self):
        node = ClusterNode(self._addr)
        node.add_slots(0)
        self.assertDictEqual(node._slots, {0: False})
        node.add_slots(range(1, 5461))
        self.assertDictEqual(node._slots, {k: False for k in range(0, 5461)})
        self.assertTrue(node._dirty)

    def test_flush_node_config(self):
        node = ClusterNode(self._addr)
        mock_config = {'cluster.return_value': self._cluster_nodes}
        _r = fake_redis(**mock_config)
        with patch.object(node, '_r', _r):
            node.load_info()
            node._dirty = True
            node.flush_node_config()
            _r.cluster.assert_called_with("ADDSLOTS", *list(range(5461, 11265)))
             

    def tearDown(self):
        pass
