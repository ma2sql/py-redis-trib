import unittest
from redis_trib.mixins.check_cluster import Node, Nodes, CheckCluster
from redis_trib.trib import RedisTrib
from copy import deepcopy
from . import fixture

## TODOs
# - open slot에 대한 판단 검증


class TestCheckCluster(unittest.TestCase):

    def setUp(self):
        def _make_mynode(nodes, idx):
            new_nodes = deepcopy(nodes)
            mynode = new_nodes[idx]
            mynode['flags'] = f"myself,{mynode['flags']}"
            return new_nodes

        default_nodes_info = fixture.cluster_nodes()
        self._nodes = []
        for i, _ in enumerate(default_nodes_info):
            mynodes =  _make_mynode(default_nodes_info, i)
            self._nodes.append(Node(mynodes[i]['addr'], mynodes[i], mynodes[:i] + mynodes[i+1:]))

        self._check_cluster = CheckCluster(Nodes(self._nodes))


    def testCheckOpenSlots(self):
        self.assertSetEqual(self._check_cluster.check_open_slots(), {1})


    def testConfigConsistency(self):
        self.assertEqual(self._nodes[0].config_signature(),
                         'abc:0-5460|def:5461-10922|ghi:10923-16383')
       
    def testSlotCoverage(self):
        self.assertListEqual(self._check_cluster.check_slots_coverage(), [])

    def testSignatureConsistency(self):
        self.assertTrue(self._check_cluster.is_config_consistent())

    def tearDown(self):
        pass

