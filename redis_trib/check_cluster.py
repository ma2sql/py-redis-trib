from .util import xprint
from .const import CLUSTER_HASH_SLOTS


class CheckCluster:
    def __init__(self, nodes):
        self._nodes = nodes

    def check_config_consistency(self):
        if not self._nodes.is_config_consistent():
            xprint("[ERR] Nodes don't agree about configuration!")
        else:
            xprint("[OK] All nodes agree about slots configuration.")

    def check_open_slots(self):
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

    def check_slots_coverage(self):
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
