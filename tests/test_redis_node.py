import unittest
from unittest.mock import Mock, MagicMock, patch

from redis_trib import (
    ClusterNode,
    AssertClusterError,
    AssertEmptyError
)

def fake_redis():
    mock = Mock()
    mock.info.return_value = {'cluster_enabled': 1}
    mock.cluster.return_value
    return mock

class TestClusterNode(unittest.TestCase):
    def setUp(self):
        self._addr = '192.168.56.101:7001'

    def testConnectRedis(self):
        with patch('redis.StrictRedis') as redis:
            node = ClusterNode(self._addr)
            node.connect()
            redis.assert_called()
   
    def testAssertCluster(self):
        redis_mock = Mock()
        redis_mock.info.return_value = {'cluster_enabled': 1}
        node = ClusterNode(self._addr)
        with patch.object(node, '_r', redis_mock):
            node.assert_cluster()

    def testAssertNotCluster(self):
        redis_mock = Mock()
        redis_mock.info.return_value = {'cluster_enabled': 0}
        node = ClusterNode(self._addr)
        with patch.object(node, '_r', redis_mock):
            with self.assertRaises(AssertClusterError):
                node.assert_cluster()

    def testAssertEmpty(self):
        redis_mock = Mock()
        redis_mock.info.return_value = {'db0': {}}
        redis_mock.cluster.return_value = {'cluster_known_nodes': 1}

        node = ClusterNode(self._addr)
        with patch.object(node, '_r', redis_mock):
            node.assert_empty()
        
    def testAssertNotEmpty(self):
        redis_mock = Mock()
        redis_mock.info.return_value = {}
        redis_mock.cluster.return_value = {'cluster_known_nodes': 0}

        node = ClusterNode(self._addr)
        with patch.object(node, '_r', redis_mock):
            with self.assertRaises(AssertEmptyError):
                node.assert_empty()

    def testLoadInfo(self):
        cluster_nodes = {
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
            }
        }
        redis_mock = Mock()
        redis_mock.cluster.return_value = cluster_nodes

        node = ClusterNode(self._addr)
        with patch.object(node, '_r', redis_mock):
            node.load_info()
               


    def tearDown(self):
        pass
