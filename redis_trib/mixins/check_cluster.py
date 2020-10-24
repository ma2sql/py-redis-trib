from ..util import summarize_slots
from ..const import CLUSTER_HASH_SLOTS
from ..xprint import xprint


def summarize_slots(slots):
    _temp_slots = []
    for slot in sorted(slots):
        if not _temp_slots or _temp_slots[-1][-1] != (slot-1): 
            _temp_slots.append([])
        _temp_slots[-1][1:] = [slot]
    return ','.join(map(lambda slot_exp: '-'.join(map(str, slot_exp)), _temp_slots)) 


def parse_slots(slots):
    parsed_slots = []
    for s in slots:
        # ["0", "5460"]
        if len(s) == 2:
            start = int(s[0])
            end = int(s[1]) + 1
            parsed_slots += list(range(start, end))
        # ["5462"]
        else:
            parsed_slots += [int(s[0])]

    return parsed_slots


class Node:
    def __init__(self, addr, node, friends):
        self._addr = addr
        self._node = node
        self._friends = friends or []

    def __str__(self):
        return self._addr

    @property
    def node_id(self):
        return self._node.get('node_id')

    @property
    def migrating(self):
        return self._node.get('migrating')

    @property
    def importing(self):
        return self._node.get('importing')

    @property
    def friends(self):
        return self._friends 

    @property
    def slots(self):
        return parse_slots(self._node['slots']) 

    @property
    def flags(self):
        return self._node.get('flags').split(',')
   
    def config_signature(self):
        signature = []
        for n in [self._node] + self._friends:
            signature.append(f"{n['node_id']}:{summarize_slots(parse_slots(n['slots']))}")
        return '|'.join(sorted(signature))

    def cluster_count_keys_in_slot(self, slot):
        return None
            

class Nodes:
    def __init__(self, nodes):
        self._nodes = nodes

    @property
    def covered_slots(self):
        return set(slot for n in self
                        for slot in n.slots)

    def __iter__(self):
        for node in self._nodes:
            yield node

    def __eq__(self, _nodes):
        return self._nodes == _nodes

    @property
    def masters(self):
        for node in self:
            if 'master' in node.flags:
                yield node


IMPORTING = 'importing'
MIGRATING = 'migrating'


class CheckCluster:

    def __init__(self, nodes=None):
        self._nodes = nodes

    def check(self, quiet=False):
        if not quiet:
            self._show_nodes()
        self._check_config_consistency()
        self._check_open_slots()
        self._check_slots_coverage()

    def _check_config_consistency(self):
        if not self._is_config_consistent():
            self._increase_num_errors()
            xprint.error("Nodes don't agree about configuration!")
        else:
            xprint.ok("All nodes agree about slots configuration.")

    def check_open_slots(self):
        opened_slots = set()
        for node in self._nodes:
            for open_type in [MIGRATING, IMPORTING]:
                slots = getattr(node, open_type) 
                if slots:
                    xprint.warning(self._warn_opened_slot(node, open_type, slots.keys()))
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

    def check_slots_coverage(self):
        return list(set(range(CLUSTER_HASH_SLOTS)) - self._nodes.covered_slots)

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

    def is_config_consistent(self):
        return len(set(n.config_signature() for n in self._nodes)) == 1


class FixCluster:

    def __init__(self, nodes):
        self._nodes = nodes

    def fix_open_slot(self, slot):
        return self._nodes, slot

    def _slot_owners(self, slot):
        owners = []
        for n in self._nodes.masters:
            if slot in n.slots:
                owners.append(n)
            elif n.cluster_count_keys_in_slot(slot):
                owners.append(n)

        return owners
