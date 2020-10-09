import unittest
from redis_trib.cluster_node import Node
from redis_trib.trib import RedisTrib

## TODOs
# - open slot에 대한 판단 검증

class TestFixCluster(unittest.TestCase):
    def setUp(self):
        self._node1 = Node('192.168.56.101:6379')
        self._node1._migrating = {'1': 'abc'}
        self._node2 = Node('192.168.56.102:6379')
        self._node2._importing = {'1': 'efg'}

    # - open slot에 대한 주인 찾기
    def testOpenedSlots(self):
        self.assertIn('1', self._node1.migrating.keys())
        self.assertIn('1', self._node2.importing.keys())
        self.assertNotIn('2', self._node1.migrating.keys())
        self.assertNotIn('2', self._node2.importing.keys())


    def testCheckOpenSlots(self):
        trib = RedisTrib([self._node1, self._node2])
        open_slots = trib._check_open_slots()
        self.assertSetEqual(open_slots, {'1'})


    def tearDown(self):
        pass

