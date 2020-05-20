import itertools
import more_itertools
import abc
import random
import time

from ..const import CLUSTER_HASH_SLOTS
from ..util import xprint, group_by
from ..cluster_node import ClusterNode


class CreateCluster:

    __slots__ = ()

    def create_cluster(self, role_distribution):
        xprint(">>> Creating cluster")
        role_distribution.distribute()

        xprint(f">>> Performing hash slots allocation "
               f"on {len(self._get_masters())} nodes...")
        self._alloc_slots()
        self._show_nodes()

        # yes_or_die "Can I set the above configuration?"
        self._flush_nodes_config()

        xprint(">>> Nodes configuration updated")
        xprint(">>> Assign a different config epoch to each node")
        self._assign_config_epoch()

        xprint(">>> Sending CLUSTER MEET messages to join the cluster")
        self._join_all_cluster()

        time.sleep(1)
        self._wait_cluster_join()
        self._flush_nodes_config()

    def _alloc_slots(self):
        masters = self._get_masters()
        slots_per_node = float(CLUSTER_HASH_SLOTS) / len(masters)
        first = 0
        cursor = 0.0
        for i, m in enumerate(masters):
            last = round(cursor + slots_per_node - 1)
            if last > CLUSTER_HASH_SLOTS or i == len(masters) - 1:
                last = CLUSTER_HASH_SLOTS - 1
            if last < first:
                last = first
            m.add_slots(list(range(first, last+1)))
            first = last+1
            cursor += slots_per_node

    def _get_role_distribution_strategy(self, nodes, user_custom, replicas):
        if user_custom:
            return CustomRoleDistribution(nodes)
        return OriginalRoleDistribution(nodes, replicas)
        
    def _flush_nodes_config(self):
        for n in self._nodes:
            n.flush_node_config()

    def _assign_config_epoch(self):
        for config_epoch, m in enumerate(self._get_masters(), 1):
            xprint(f"[WARNING] {m}: {config_epoch}")
            m.set_config_epoch(config_epoch)

    def _join_all_cluster(self):
        self._join_cluster(self._nodes[1:])
