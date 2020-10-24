import unittest
from redis_trib.mixins.check_cluster import Node, Nodes, CheckCluster, FixCluster
from redis_trib.trib import RedisTrib
from copy import deepcopy
from . import fixture


class TestFixCluster(unittest.TestCase):

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
        self._fix_cluster = FixCluster(Nodes(self._nodes))

    def testFixOpenSlot(self):
        nodes, slot = self._fix_cluster.fix_open_slot(1)
        self.assertEqual(slot, 1)
        self.assertTrue(Nodes(self._nodes) == nodes)

    def testSlotOwners(self):
        owners = self._fix_cluster._slot_owners(1)
        self.assertListEqual(owners, [self._nodes[0]])

    def tearDown(self):
        pass

