import unittest
from redis_trib.mixins.check_cluster import (
    Node, Nodes, CheckCluster, FixCluster, FixOpenSlot, 
    FixOpenSlotNoOwner, FixOpenSlotMultipleOwner
)
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

    def testMovingSlots(self):
        migrating, importing = self._fix_cluster.moving_slots(1, None)
        self.assertDictEqual(migrating[0].migrating, {1: '2bd45a5a7ec0b5cb316d2e9073bb84c7ba81eea3'})
        self.assertDictEqual(importing[0].importing, {1: '54b3cd517c7ce508630b9c9366cd4da19681fee7'})

    def testSlotOwnerStrategy(self):
        no_owner_strategy = self._fix_cluster.fix_open_slot_strategy([])
        self.assertIs(no_owner_strategy, FixOpenSlotNoOwner)

        multiple_owner_strategy = self._fix_cluster.fix_open_slot_strategy(self._nodes[:2])
        self.assertIs(multiple_owner_strategy, FixOpenSlotMultipleOwner)

    def testNodeWithMostKeysInSlot(self):
        fix_open_slot = FixOpenSlot()
        self.assertEqual(fix_open_slot.get_node_with_most_keys_in_slot(self._nodes, 1), self._nodes[0])

    def tearDown(self):
        pass

