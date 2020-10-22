import unittest
from redis_trib.mixins.check_cluster import Node, Nodes, CheckCluster
from redis_trib.trib import RedisTrib
from copy import deepcopy
from . import fixture


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
                         '2bd45a5a7ec0b5cb316d2e9073bb84c7ba81eea3:5461-10922|'\
                         '54b3cd517c7ce508630b9c9366cd4da19681fee7:0-5460|'\
                         '91d5f362ba127c8e0aba925f8e005f8b08054042:10923-16383')
       
    def testSlotCoverage(self):
        self.assertListEqual(self._check_cluster.check_slots_coverage(), [])

    def testSignatureConsistency(self):
        self.assertTrue(self._check_cluster.is_config_consistent())

    def tearDown(self):
        pass

