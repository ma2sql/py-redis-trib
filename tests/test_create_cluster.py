import unittest
from unittest.mock import patch

from redis_trib import (
    RedisTrib,
    ClusterNode,
    TooSmallMastersError,
)

import redis
from itertools import chain
from more_itertools import flatten

'''
## TODOs
- Implement alloc_slots
	[v] ClusterNode
		- add_slots
	[v] Initialize ClusterNode
	[v] Calculate slots per node
	[ ] Divide master and replicas
'''

class TestCreateCluster(unittest.TestCase):
    def setUp(self):
        self._node_addrs = [
            '192.168.56.101:7000',
            '192.168.56.102:7000',
            '192.168.56.103:7000',
            '192.168.56.104:7000',
            '192.168.56.105:7000',
            '192.168.56.106:7000',
        ]

        self._node_addrs_with_slaves = [
            '192.168.56.101:7000,192.168.56.101:7001',
            '192.168.56.102:7000,192.168.56.102:7001',
            '192.168.56.103:7000,192.168.56.103:7001',
            '192.168.56.104:7000,192.168.56.104:7001',
            '192.168.56.105:7000,192.168.56.105:7001',
            '192.168.56.106:7000,192.168.56.106:7001',
        ]

    def test_connect_to_all_redis(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        redis_trib._distribute_roles()
        patch('redis.StrictRedis')
        with patch.object(ClusterNode, 'connect') as f:
            redis_trib._connect_to_nodes()
            self.assertEqual(f.call_count, len(redis_trib._nodes))

    def test_alloc_slots(self):
        redis_trib = RedisTrib(self._node_addrs)
        redis_trib._distribute_roles()
        redis_trib._alloc_slots()
        self.assertListEqual(list(flatten(m.slots for m in redis_trib._masters)),
                             list(range(0, 16384)))


    def test_distribute_roles(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        redis_trib._distribute_roles()

        all_nodes_addrs = sum([addr.split(',') for addr in self._node_addrs_with_slaves], [])
        self.assertListEqual(
            [n.addr for n in redis_trib._nodes], all_nodes_addrs)

    def test_check_parameters(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        with self.assertRaises(TooSmallMastersError):
            redis_trib._check_parameters()

        redis_trib._distribute_roles()
        redis_trib._check_parameters()

    def test_flush_nodes_config(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        redis_trib._distribute_roles()
        with patch.object(ClusterNode, 'flush_node_config') as f:
            redis_trib._flush_nodes_config()
            self.assertEqual(f.call_count, len(redis_trib._nodes))

    def test_assign_config_epoch(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        redis_trib._distribute_roles()
        with patch.object(ClusterNode, 'assign_config_epoch') as f:
            redis_trib._assign_config_epoch()
            self.assertEqual(f.call_count, len(redis_trib._masters))

    def test_join_cluster(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        redis_trib._distribute_roles()
        with patch.object(ClusterNode, 'cluster_meet') as f:
            redis_trib._join_cluster()
            self.assertEqual(f.call_count, len(redis_trib._nodes) - 1)

    def test_config_consistent(self):
        redis_trib = RedisTrib(self._node_addrs_with_slaves)
        redis_trib._distribute_roles()
        with patch.object(ClusterNode, 'get_config_signature') as f:
            redis_trib._is_config_consistent()
            self.assertEqual(f.call_count, len(redis_trib._nodes))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
