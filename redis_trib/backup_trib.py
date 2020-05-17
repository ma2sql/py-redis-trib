from .util import xprint
from .const import CLUSTER_HASH_SLOTS
import time

class RedisTrib:
    def __init__(self, nodes):
        self._nodes = nodes

    def create(self, role_distribution_strategy, replicas=0):
        xprint(">>> Creating cluster")
        role_distribution = role_distribution_strategy(self._nodes, replicas)
        role_distribution.distribute()

        self._nodes.evaluate_anti_affinity()

        xprint(f">>> Performing hash slots allocation "
               f"on {len(self._nodes.masters)} nodes...")
        self._nodes.alloc_slots()
        self._nodes.show_nodes()

        # yes_or_die "Can I set the above configuration?"
        self._nodes.flush_nodes_config()

        xprint(">>> Nodes configuration updated")
        xprint(">>> Assign a different config epoch to each node")
        self._nodes.assign_config_epoch()

        xprint(">>> Sending CLUSTER MEET messages to join the cluster")
        self._nodes.join_cluster()

        time.sleep(1)
        self._nodes.wait_cluster_join()
        self._nodes.flush_nodes_config()

    def show_cluster_info(self):
        masters = 0
        keys = 0
        for n in self._nodes:
            if 'master' in n.flags:
                print(f"{n} ({n.node_id[:8]}...) -> {n.dbsize} keys "\
                      f"| {len(n.slots)} slots | {len(n.replicas)} slaves.")
                masters += 1
                keys += n.dbsize
        xprint(f"[OK] {keys} keys in {masters} masters.")
        print(f"{keys/16384.0:.2f} keys per slot on average.")

    def check_cluster(self):
        self._nodes.show_nodes()
        self._check_config_consistency()
        self._check_open_slots()
        self._check_slots_coverage()

    def _check_config_consistency(self):
        if not self._nodes.is_config_consistent():
            xprint("[ERR] Nodes don't agree about configuration!")
        else:
            xprint("[OK] All nodes agree about slots configuration.")

    def _check_open_slots(self):
        xprint(">>> Check for open slots...")
        open_slots = {}
        for n, migrating, importing in self._nodes.opened_slots():
            if len(migrating) > 0:
                xprint(self._warn_opened_slot(n, 'migrating', migrating.keys()))
                open_slots.union(set(migrating.keys()))
            if len(importing) > 0:
                xprint(self._warn_opened_slot(n, 'importing', importing.keys()))
                open_slots.union(set(importing.keys()))
        if len(open_slots) > 0:
            xprint(f"[WARNING] The following slots are open: "\
                   f"{','.join(open_slots)}")
        return open_slots

    def _check_slots_coverage(self):
        xprint(">>> Check slots coverage...")
        covered_slots = self._nodes.covered_slots()
        if len(covered_slots) == CLUSTER_HASH_SLOTS:
            xprint(f"[OK] All {CLUSTER_HASH_SLOTS} slots covered.")
        else:
            xprint(f"[ERR] Not all {CLUSTER_HASH_SLOTS} {covered_slots} slots are covered by nodes.")

        return list(range(CLUSTER_HASH_SLOTS)) - covered_slots.keys()

    def _warn_opened_slot(self, node, open_type, slots):
        return f"[WARNING] Node {node} has slots in {open_type} "\
               f"state {','.join(slots)}"

    def add_node(self, new_node):
        xprint(f">>> Adding node {argv[0]} to cluster {argv[1]}")
