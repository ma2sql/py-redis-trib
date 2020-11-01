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
        return self._node.get('migrating') or {}

    @property
    def importing(self):
        return self._node.get('importing') or {}

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
        return 0

    def clear_slot(self, slot):
        # CLUSTER SETSLOT __slot__ STABLE
        pass

    def set_slot_owner(self, slot):
        '''
        self._r.pipeline()\
            .cluster('DELSLOTS', slot)\
            .cluster('ADDSLOTS', slot)\
            .cluster('BUMPEPOCH')\
            .execute() 
        '''

    def cluster_bumpepoch(self):
        '''
        CLUSTER BUMPEPOCH
        '''

    def cluster_setslot_stable(self, slot):
        pass

    def cluster_setslot_node(self, slot, owner):
        pass

    def cluster_setslot_migrating(self, slot, target):
        pass
 
    def cluster_setslot_importing(self, slot, source):
        pass

    def cluster_delslots(self, slots):
        pass

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

class MoveSlot:
    def __init__(self, src, dst, slot):
        self._src = src
        self._dst = dst
        self._slot = slot

    def move_slot(self):
        return self

    def moving(self):
        return self

    def update(self):
        return self

    def notify(self):
        return self


class FixCluster:

    def __init__(self, nodes):
        self._nodes = nodes


class FixOpenSlot:
    def __init__(self, nodes, slot):
        self._nodes = nodes
        self._owner = None
        self._owners = []
        self._migrating = []
        self._importing = []


    def get_node_with_most_keys_in_slot(self, nodes, slot):
        best = None
        best_numkeys = 0

        for n in nodes:
            numkeys = n.cluster_count_keys_in_slot(slot)
            if numkeys > best_numkeys or best is None:
                numkeys = n.cluster_count_keys_in_slot(slot)
                best = n
                best_numkeys = numkeys

        return best


    def fix_open_slot(self, slot):
        self.set_slot_owners(slot)
        owner = self.elect_owner()
        self.change_owner_to_donor()

        if (len(self._migrating) == 1
            and len(self._importing) == 1):
            src = self._migrating[0]
            dst = self._importing[0]
            move_slot(src, dst, slot, update=True)
        elif (len(self._migrating) == 0
              and len(self._importing) > 0):
            for n in self._importing:
                if n == owner:
                    continue
                move_slot(n, owner, slot, cold=True)
                n.clear_slot(slot)
                m = MoveSlot(n, owner, slot)
                m.move_slot()
        elif (len(self._migrating) == 1
              and len(self._importing) > 1):
            src = self._migrating[0]
            dst = None
            target_id = src.migrating['id']
            for n in self._importing:
                if n.cluster_count_keys_in_slot(slot) > 0:
                    raise BaseException()
                if target_id == n.node_id:
                    dst = n
            if dst:
                move_slot(src, dst, slot)
                for n in self._importing:
                    if n == dst:
                        continue
                    n.clear_slot(slot)
            else:
                src.clear_slot(slot)
                for n in self._importing:
                    n.clear_slot(n)
            






    def set_owner_candidates(self):
        self._owners = []
        for n in self._nodes.masters:
            if slot in n.slots:
                self._owners.append(n)
            elif n.cluster_count_keys_in_slot(slot):
                self._owners.append(n)

    def elect_owner(self):
        owner = self._get_node_with_most_keys_in_slot(self._nodes, slot)
        owner.clear_slot(slot)
        owner.set_slot_owner(slot)
        owner.add_slots(slot)
        owner.cluster_bumpepoch()
        self._importing = [n for n in self._importing if n != owner]
        self._migrating = [n for n in self._migrating if n != owner]

    def change_owner_to_donor(self):
        if self._owner is None:
            raise BaseException('No owner')
        for n in self._owners:
            if n == self._owner:
                continue
            n.del_slots(slot)
            n.cluster_setslot_stable(slot)
            n.cluster_setslot_importing(slot, owner)
            self._importing = [_n for _n in self._importing if _n != n] 
            self._importing.append(n)
            self._migrating = [_n for _n in self._migrating if _n != n]

    def moving_slots(self, slot, owner):
        self._migrating = []
        self._importing = []

        for n in self._nodes.masters:
            if n.migrating.get(slot):
                self._migrating.append(n)
            elif n.importing.get(slot):
                self._importing.append(n)
            elif n != owner and n.cluster_count_keys_in_slot(slot) > 0:
                self._importing.append(n)


    def fix_open_slot_strategy(self, owners):
        # owners 구하기
        # owner is None ?
        # 1) owners가 0일 때,
        # 2) owners가 2 이상일 때
        # 즉, owners가 1이 아닐 때
        if not owners:
            return FixOpenSlotNoOwner
        return FixOpenSlotMultipleOwner


class FixOpenSlot:
    def __init__(self):
        pass

    def get_node_with_most_keys_in_slot(self, nodes, slot):
        best = None
        best_numkeys = 0

        for n in nodes:
            numkeys = n.cluster_count_keys_in_slot(slot)
            if numkeys > best_numkeys or best is None:
                numkeys = n.cluster_count_keys_in_slot(slot)
                best = n
                best_numkeys = numkeys

        return best
        


class FixOpenSlotNoOwner(FixOpenSlot):
    def __init__(self, nodes, fix_cluster, slot):
        super().__init__(self)
        self._nodes = nodes
        self._fix_cluster = fix_cluster
        self._slot = slot

    def _get_owner(self):
        owner = self._get_node_with_most_keys_in_slot(
                    self._nodes.get_masters(), self._slot)

    def fix(self):
        owner = self._get_owner()
        if not owner:
            raise FixOpenSlotError("[ERR] Can't select a slot owner. Impossible to fix.")

        xprint(f"*** Configuring {owner} as the slot owner")

        # clear
        owner.cluster_setslot_stable(slot)

        with r.pipeline(transaction=True) as t:
            t.cluster('DELSLOTS', slot)
            t.cluster('ADDSLOTS', slot)
            t.cluster('SETSLOT', slot, 'STABLE')
            t.cluster('BUMPEPOCH')
            results = t.execute(raise_on_error=False)
       
        delslot_err, *remain_err = results
        if isinstance(delslot_err, redis.ResponseError):
            if not str(delslot_err).endswith('already unassigned'):
                raise BaseException('ERROR!')
        if any(isinstance(err, redis.ResponseError) for err in remain_err):
            raise BaseException('ERROR!')

        owner.add_slots(slot, new=False)
        owner.cluster_bumpepoch()
        # Remove the owner from the list of migrating/importing
        # nodes.
        migrating.remove(owner)
        importing.remove(owner)



class FixOpenSlotMultipleOwner(FixOpenSlot):
    def __init__(self):
        super().__init__(self)
    
