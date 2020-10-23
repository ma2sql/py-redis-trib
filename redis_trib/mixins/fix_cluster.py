from ..util import xprint, chunk
from ..const import CLUSTER_HASH_SLOTS
from more_itertools import divide
import redis

class FixCluster:

    __slots__ = ()

    def fix(self):
        self.fix_slots_coverage()
        open_slots = self._check_open_slots()
        for slot in open_slots:
            self.fix_open_slot(slot)


    def fix_slots_coverage(self):
        xprint(">>> Fixing slots coverage...")
        not_covered = set(range(CLUSTER_HASH_SLOTS)) - self._get_covered_slots()

        slots = {}
        for slot in not_covered:
            #n = self._get_nodes_with_keys_in_slot(slot)
            n = []
            slots[slot] = n
            xprint(f"Slot {slot} has keys in {len(n)} nodes: {', '.join(n)}")

        none = {}
        single = {}
        multi = {} 
        for slot, nodes in sorted(slots.items(), key=lambda x: x[0]):
            l = len(nodes)
            if l == 0:
                none.update({slot: nodes})
            elif l == 1: 
                single.update({slot: nodes})
            else:
                multi.update({slot: nodes})

        # Handle case "1": keys in no node.
        if len(none) > 0:
            xprint(f"The folowing uncovered slots have no keys across the cluster:")
            xprint(f"Fix these slots by covering with a random node?")
            # chunk단위로 none내의 slot을 자름
            # random하게 분배하지 않고, slot range별로 분배시킨다.
            sorted_slots = sorted(none.keys())
            tot_none_slots = len(sorted_slots)
            for node, slots in zip(self._get_masters(),
                                   divide(len(self._get_masters()), sorted_slots)):
                for slot in slots:
                    xprint(f">>> Covering slot {slot} with {node}")
                    with node.r.pipeline(transaction=True) as t:
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
    
        if len(single) > 0:
            xprint("The folowing uncovered slots have keys in just one node:")
            print(','.join(single.keys()))
            query_yes_no("Fix these slots by covering with those nodes?")
            for slot, nodes in single.items(): 
                node = nodes[0]
                xprint(f">>> Covering slot {slot} with {node}")
                node.r.cluster("addslots",slot)

                with n.r.pipeline(transaction=True) as t:
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
                    
        if len(multi) > 0:
            xprint("The folowing uncovered slots have keys in multiple nodes:")
            print(",".join(multi.keys()))
            query_yes_no("Fix these slots by moving keys into a single node?")
            for slot, nodes in multi.items():
                target = self._get_node_with_most_keys_in_slot(nodes, slot)
                target.add_slots(slot, new=False)
                xprint(f">>> Covering slot {slot} moving keys to {target}")

                # clusterManagerSetSlotOwner
                with n.r.pipeline(transaction=True) as t:
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
                    
                for src in nodes:
                    if src == target:
                        continue
                    # Set the source node in 'importing' state (even if we will
                    # actually migrate keys away) in order to avoid receiving
                    # redirections for MIGRATE.
                    src.cluster_setslot_importing(slot, target)
                    self._move_slot(src, target, slot, dots=True, fix=True, cold=True)
                    src.cluster_setslot_stable(slot)

             
    def _get_nodes_with_keys_in_slot(self, slot):
        nodes = []
        for n in self._get_masters():
            if n.cluster_count_keys_in_slot(slot) > 0:
                nodes.append(n)
        return nodes

    def fix_open_slot(self, slot, force_fix=False):
        xprint(f">>> Fixing open slot {slot}")

        if self._unreachable_masters > 0 and not force_fix:
            xprint(f"*** Fixing open slots with {self._unreachable_masters} "
                   f"unreachable masters is dangerous: "
                   f"redis-cli will assume that slots about masters "
                   f"that are not reachable are not covered, "
                   f"and will try to reassign them to the reachable nodes. "
                   f"This can cause data loss and is rarely what you want to do. "
                   f"If you really want to proceed use "
                   f"the --cluster-fix-with-unreachable-masters option.")

        owners = []
        for n in self._get_masters():
            if n.slots.get(slot):
                owners.append(n)
            elif n.cluster_count_keys_in_slot(slot):
                owners.append(n)

        # Try to obtain the current slot owner, according to the current
        # nodes configuration.
        owners = self._get_slot_owners(slot)
        if len(owners) == 1:
            owner = owners[0]

        migrating = []
        importing = []

        for n in self._get_masters():
            if n.migrating.get(slot):
                migrating.append(n)
            elif n.importing.get(slot):
                importing.append(n)
            elif n != owner and n.cluster_count_keys_in_slot(slot) > 0:
                xprint(f"*** Found keys about slot {slot} in node {n}!")
                importing.append(n)
     
        xprint(f"Set as migrating in: {','.join(map(str, migrating))}")
        xprint(f"Set as importing in: {','.join(map(str, importing))}")

        # If there is no slot owner, set as owner the slot with the biggest
        # number of keys, among the set of migrating / importing nodes.
        if not owner:
            xprint(">>> Nobody claims ownership, selecting an owner...")
            owner = self._get_node_with_most_keys_in_slot(self._get_masters(), slot)

            # If we still don't have an owner, we can't fix it.
            if not owner:
                xprint("[ERR] Can't select a slot owner. Impossible to fix.")
                # exit 1

            # Use ADDSLOTS to assign the slot.
            print(f"*** Configuring {owner} as the slot owner")

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

        # If there are multiple owners of the slot, we need to fix it
        # so that a single node is the owner and all the other nodes
        # are in importing state. Later the fix can be handled by one
        # of the base cases above.
        #
        # Note that this case also covers multiple nodes having the slot
        # in migrating state, since migrating is a valid state only for
        # slot owners.
        if len(owners) > 1:
            owner = self._get_node_with_most_keys_in_slot(owners, slot)
            for n in owners:
                if n == owner:
                    continue
                n.cluster_delslots(slot)
                n.del_slot(slot)
                n.cluster_setslot_node(slot, owner)
                n.cluster_setslot_importing(slot, owner)

                # Avoid duplciates
                importing.remove(n)
                importing.append(n)

                # Ensure that the node is not in the migrating list.
                migrating.remove(n)

            owner.cluster_bumpepoch()

        # Case 1: The slot is in migrating state in one slot, and in
        #         importing state in 1 slot. That's trivial to address.
        if len(migrating) == 1 and len(importing) == 1:
            xprint(f">>> Case 1: Moving slot {slot} from "
                   f"from {migrating[0]} to {importing[0]}")
            self._move_slot(migrating[0], importing[0], slot, update=True)

        # Case 2: There are multiple nodes that claim the slot as importing,
        # they probably got keys about the slot after a restart so opened
        # the slot. In this case we just move all the keys to the owner
        # according to the configuration.
        elif len(migrating) == 0 and len(importing) > 0:
            xprint(f">>> Moving all the {slot} slot keys to its owner {owner}")
            for n in importing:
                if n == owner:
                    continue
                self._move_slot(n, owner, slot, cold=True)
               
                xprint(f">>> Setting {slot} as STABLE in {node}")
                n.cluster_setslot_stable(slot)

            # Since the slot has been moved in "cold" mode, ensure that all the
            # other nodes update their own configuration about the slot itself.
            for n in self._get_masters():
                if n == owner:
                    continue
                n.cluster_setslot_node(slot, owner)
            
        # Case 3: The slot is in migrating state in one node but multiple
        # other nodes claim to be in importing state and don't have any key in
        # the slot. We search for the importing node having the same ID as
        # the destination node of the migrating node.
        # In that case we move the slot from the migrating node to this node and
        # we close the importing states on all the other importing nodes.
        # If no importing node has the same ID as the destination node of the
        # migrating node, the slot's state is closed on both the migrating node
        # and the importing nodes.
        elif len(migrating) == 1 and len(importing) > 1:
            src = migrating[0]
            dst = None
            target_id = src.migrating.get(slot)
        
            for n in importing:
                num_keys = n.cluster_count_keys_in_slot(slot)
                if num_keys > 0:
                    try_to_fix = False
                    break
                if n.node_id == targe_id:
                    dst = n
                    
            if dst is not None:
                xprint(f">>> Case 3: Moving slot {slot} from {src} to "
                       f"{dst} and closing it on all the other importing nodes.")
                # Move the slot to the destination node.
                self._move_slot(src, dst, slot)
 
                for n in importing:
                    if n == dst:
                        continue
                    n.cluster_setslot_stable(slot)
 
            else:
                xprint(f">>> Case 3: Closing slot {slot} on both "
                       f"migrating and importing nodes.")
                # Close the slot on both the migrating node and the importing nodes.
                src.cluster_setslot_stable(slot)
                for n in importing:
                    n.cluster_setslot_stable(slot)
             
        else:    
            try_to_close_slot = len(importing) == 0 and len(migrating) == 1
            if try_to_close_slot:
                n = migrating[0]
                if not owner or owner != n:
                    num_keys = n.cluster_count_keys_in_slot(slot)
                    try_to_close_slot = num_keys != 0

            # Case 4: There are no slots claiming to be in importing state, but
            # there is a migrating node that actually don't have any key or is the
            # slot owner. We can just close the slot, probably a reshard
            # interrupted in the middle.
            if try_to_close_slot:
                xprint(f">>> Case 4: Closing slot {slot} on {n}")
                n.cluster_setslot_stable(slot)

            else:
                xprint(f"[ERR] Sorry, redis-trib can't fix this slot yet "
                       f"(work in progress). Slot is set "
                       f"as migrating in {migrating.join(',')}, "
                       f"as importing in {importing.join(',')}, "
                       f"owner is {owner}")


    # Return the node, among 'nodes' with the greatest number of keys
    # in the specified slot.
    def _get_node_with_most_keys_in_slot(self, nodes, slot):
        best = None
        best_numkeys = 0
        for n in nodes:
            numkeys = n.cluster_count_keys_in_slot(slot)
            if numkeys > best_numkeys or best is None:
                best = n
                best_numkeys = numkeys

        return best


