from ..util import xprint


class FixCluster:

    __slots__ = ()

    def fix_open_slot(self, slot):
        xprint(f">>> Fixing open slot {slot}")

        # TODO: Unrechable masters

        '''
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        if (n->slots[slot]) {
            listAddNodeTail(owners, n);
        } else {
            redisReply *r = CLUSTER_MANAGER_COMMAND(n,
                "CLUSTER COUNTKEYSINSLOT %d", slot);
            success = clusterManagerCheckRedisReply(n, r, NULL);
            if (success && r->integer > 0) {
                clusterManagerLogWarn("*** Found keys about slot %d "
                                      "in non-owner node %s:%d!\n", slot,
                                      n->ip, n->port);
                listAddNodeTail(owners, n);
            }
            if (r) freeReplyObject(r);
            if (!success) goto cleanup;
        }
    }
        '''
        for n in self._get_masters():
            if n.slots.get(slot):
                owners.append(n)
            else:
                not_visible_keys = n.cluster_count_keys_in_slot(slot)
                if not_visible_keys:
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
     
        xprint(f"Set as migrating in: {','.join(migrating)}")
        xprint(f"Set as importing in: {','.join(importing)}")

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


            # clusterManagerSetSlotOwner(owner, slot, 0)
    '''
    int success = clusterManagerStartTransaction(owner);
    if (!success) return 0;
    /* Ensure the slot is not already assigned. */
    clusterManagerDelSlot(owner, slot, 1);
    /* Add the slot and bump epoch. */
    clusterManagerAddSlot(owner, slot);
    if (do_clear) clusterManagerClearSlotStatus(owner, slot);
    clusterManagerBumpEpoch(owner);
    success = clusterManagerExecTransaction(owner, clusterManagerOnSetOwnerErr);
    '''
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

            owners.add_slots(slot, new=False)
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
                n.cluster('delslots',slot)
                n.cluster_setslot_importing(slot, owner.node_id)
                importing.remove(n) # Avoid duplciates
                importing.append(n)
            }
            owner.cluster_bumpepoch()

        # Case 1: The slot is in migrating state in one slot, and in
        #         importing state in 1 slot. That's trivial to address.
        if len(migrating) == 1 and len(importing) == 1:
            self._move_slot(migrating[0],importing[0],slot,:dots=>true,:fix=>true)

        # Case 2: There are multiple nodes that claim the slot as importing,
        # they probably got keys about the slot after a restart so opened
        # the slot. In this case we just move all the keys to the owner
        # according to the configuration.
        elif len(migrating) == 0 and len(importing) > 0:
            xputs ">>> Moving all the #{slot} slot keys to its owner #{owner}"
            importing.each {|node|
                next if node == owner
                move_slot(node,owner,slot,:dots=>true,:fix=>true,:cold=>true)
                xputs ">>> Setting #{slot} as STABLE in #{node}"
                node.r.cluster("setslot",slot,"stable")
            }
        # Case 3: There are no slots claiming to be in importing state, but
        # there is a migrating node that actually don't have any key. We
        # can just close the slot, probably a reshard interrupted in the middle.
        elsif importing.length == 0 && migrating.length == 1 &&
              migrating[0].r.cluster("getkeysinslot",slot,10).length == 0
            migrating[0].r.cluster("setslot",slot,"stable")
        else
            xputs "[ERR] Sorry, Redis-trib can't fix this slot yet (work in progress). Slot is set as migrating in #{migrating.join(",")}, as importing in #{importing.join(",")}, owner is #{owner}"
        end
    end

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


