import unittest
from redis_trib.mixins.check_cluster import Node, CheckOpenSlot, CheckCluster
from redis_trib.trib import RedisTrib

## TODOs
# - open slot에 대한 판단 검증

class TestFixCluster(unittest.TestCase):
    def setUp(self):
        self._node1 = Node({'addr': '192.168.56.101:6789', 'migrating': {1: 'abc'}})
        self._node2 = Node({'addr': '192.168.56.101:6789', 'importing': {1: 'efg'}})

    # - open slot에 대한 주인 찾기
    def testOpenedSlots(self):
        self.assertIn(1, self._node1.migrating.keys())
        self.assertIn(1, self._node2.importing.keys())
        self.assertNotIn(2, self._node1.migrating.keys())
        self.assertNotIn(2, self._node2.importing.keys())

    def testCheckOpenSlot(self):
        check_migrating = CheckOpenSlot('migrating')
        opened_slot = check_migrating.check_open_slot(self._node1)
        self.assertDictEqual(opened_slot, {1: 'abc'})
        check_importing = CheckOpenSlot('importing')
        opened_slot = check_importing.check_open_slot(self._node2)
        self.assertDictEqual(opened_slot, {1: 'efg'})

    def testCheckOPenSlots(self):
        check =  CheckCluster()
        opened_slots = check.check_open_slots([self._node1, self._node2])
        self.assertSetEqual(opened_slots, {1})

    def tearDown(self):
        pass

