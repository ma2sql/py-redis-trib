from ..util import summarize_slots
from ..const import CLUSTER_HASH_SLOTS
from ..xprint import xprint


class Node:
    def __init__(self, addr, node):
        self._addr = addr
        self._node = node

    def __str__(self):
        return self._addr

    @property
    def migrating(self):
        return self._node.get('migrating')

    @property
    def importing(self):
        return self._node.get('importing')

    @property
    def config_signature(self):
        pass


class CheckOpenSlot:
    def __init__(self, open_type):
        self._open_type = open_type

    def check_open_slot(self, node):
        try:
            return getattr(node, self._open_type)
        except AttributeError:
            return None


IMPORTING = 'importing'
MIGRATING = 'migrating'


class CheckCluster:

    __slots__ = ()

    def check(self, quiet=False):
        if not quiet:
            self._show_nodes()
        self._check_config_consistency()
        self._check_open_slots()
        self._check_slots_coverage()

    def check_config_consistency(self, nodes):
        for n in nodes:
            xprint(n)
        return True

    def _check_config_consistency(self):
        if not self._is_config_consistent():
            self._increase_num_errors()
            xprint.error("Nodes don't agree about configuration!")
        else:
            xprint.ok("All nodes agree about slots configuration.")

    def check_open_slots(self, nodes):
        opened_slots = set()
        for node in nodes:
            for open_type in [MIGRATING, IMPORTING]:
                slots = getattr(node, open_type) 
                if slots:
                    opened_slots = opened_slots.union(set(slots.keys()))
        return opened_slots

    def _check_open_slots(self):
        xprint(">>> Check for open slots...")
        open_slots = set()
        for n, migrating, importing in self._get_opened_slots():
            if len(migrating) > 0:
                self._increase_num_errors()
                xprint.warning(self._warn_opened_slot(n, 'migrating', migrating.keys()))
                open_slots = open_slots.union(set(migrating.keys()))
            if len(importing) > 0:
                self._increase_num_errors()
                xprint.warning(self._warn_opened_slot(n, 'importing', importing.keys()))
                open_slots = open_slots.union(set(importing.keys()))

        if len(open_slots) > 0:
            xprint.warning(f"The following slots are open: "\
                           f"{','.join(map(str, open_slots))}")

        return open_slots

    def _check_slots_coverage(self):
        xprint(">>> Check slots coverage...")
        covered_slots = self._get_covered_slots()
        if len(covered_slots) == CLUSTER_HASH_SLOTS:
            xprint.ok(f"All {CLUSTER_HASH_SLOTS} slots covered.")
        else:
            self._increase_num_errors()
            xprint.error(f"Not all {CLUSTER_HASH_SLOTS} {summarize_slots(covered_slots)} "
                         f"slots are covered by nodes.")

        return list(set(range(CLUSTER_HASH_SLOTS)) - covered_slots)

    def _warn_opened_slot(self, node, open_type, slots):
        return f"Node {node} has slots in {open_type} "\
               f"state {','.join(map(str, slots))}"

        
