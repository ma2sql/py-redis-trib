import unittest
from redis_trib.mixins.check_cluster import Node, Nodes, CheckOpenSlot, CheckCluster
from redis_trib.trib import RedisTrib

## TODOs
# - open slot에 대한 판단 검증

class TestFixCluster(unittest.TestCase):

    def setUp(self):
        nodes_info = [
            {'node_id': 'abc', 'addr': '192.168.56.101:6789', 'flags': 'master', 'slots': '0-5460', 'migrating': {1: 'def'}},
            {'node_id': 'def', 'addr': '192.168.56.102:6789', 'flags': 'master', 'slots': '5461-10922', 'importing': {1: 'abc'}},
            {'node_id': 'ghi', 'addr': '192.168.56.103:6789', 'flags': 'master', 'slots': '10923-16383'},
        ]
        self._nodes = [Node(n['addr'], n) for n in nodes_info]

    def testCheckOpenSlot(self):
        check_migrating = CheckOpenSlot('migrating')
        opened_slot = check_migrating.check_open_slot(self._nodes[0])
        self.assertDictEqual(opened_slot, {1: 'def'})
        check_importing = CheckOpenSlot('importing')
        opened_slot = check_importing.check_open_slot(self._nodes[1])
        self.assertDictEqual(opened_slot, {1: 'abc'})

    def atestCheckOPenSlots(self):
        check =  CheckCluster()
        opened_slots = check.check_open_slots([self._node1, self._node2])
        self.assertSetEqual(opened_slots, {1})

    def testConfigConsistency(self):
        node = self._nodes[0]
        print(node.config_signature(self._nodes))
        
    def testSignatureConsistency(self):
        nodes = Nodes(self._nodes)
        self.assertTrue(nodes.is_config_consistent())

    def tearDown(self):
        pass

