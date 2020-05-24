from math import ceil, floor
from ..util import query_yes_no

class IsNotMasterNode(Exception): pass
class NotExistNode(Exception): pass

class ReshardCluster:

    __slots__ = ()

    def reshard_cluster(self, from_ids, to_id, pipeline, timeout, num_slots, yes=False):
        target = self._get_master_by_id(to_id)
        sources = [self._get_master_by_id(_id)
                   for _id in from_ids.split(',') or []]
        
        if not sources:
            sources = self._get_masters()

        # Ensure that target don't exist in sources
        sources = [n for n in sources if n != target]

        print(f"\nReady to move {num_slots} slots.")
        print(f"  Source nodes:")
        for s in sources:
            print("    {s.info_string()}")
        print(f"  Destination node:")
        print(f"     {target.info_string()}")

        reshard_table = self._compute_reshard_table(sources, num_slots)
        print(f"  Resharding plan:")
        self._show_reshard_table(reshard_table)

        if yes or query_yes_no("Do you want to proceed with "\
                               "the proposed reshard plan? ", default=True):
            for source, slot in reshard_table:
                self._move_slot(source, target, slot,
                                dots=True, pipeline=pipeline)

    # Move slots between source and target nodes using MIGRATE.
    #
    # Options:
    # :verbose -- Print a dot for every moved key.
    # :fix     -- We are moving in the context of a fix. Use REPLACE.
    # :cold    -- Move keys without opening slots / reconfiguring the nodes.
    # :update  -- Update nodes.info[:slots] for source/target nodes.
    # :quiet   -- Don't print info messages.
    def _move_slot(self, source, target, slot, pipeline, **opt):
        # We start marking the slot as importing in the destination node,
        # and the slot as migrating in the target host. Note that the order of
        # the operations is important, as otherwise a client may be redirected
        # to the target node that does not yet know it is importing this slot.
        if not opt.get('quiet'):
            print(f"Moving slot {slot} from {source} to {target}: ", end="")

        if opt.get('cold'):
            target.cluster_setslot_importing(slot, source.node_id)
            source.cluster_setslot_migrating(slot, target.node_id)

        # Migrate all the keys from source to target using the MIGRATE command
        while (keys_in_slot := source.cluster_get_keys_in_slot(slot, pipeline)):
            try:
                source.migrate(target.host, target.port, keys_in_slot, timeout, auth=self._auth)
            except redis.exceptions.ResponseError as e:
                if o('fix') and str(e).find('BUSYKEY'):
                    xprint("*** Target key exists. Replacing it for FIX.")
                    source.migrate(target.host, target.port, keys_in_slot,
                        timeout, auth=self._auth, replace=True)
                else:
                    print("")
                    xprint(f"[ERR] Calling MIGRATE: {e}")

            if dots:
                print("." * len(keys.length))


        # Set the new node as the owner of the slot in all the known nodes.
        if not opt.get('quiet'):
            print()

        #if opt.get('cold') is not None:
        if True:
            for n in self._nodes:
                if n.is_master():
                    n.cluster_setslot_node(slot, target)

        # Update the node logical config
        if opt.get('update'):
            source.del_slots(slot)
            target.add_slots(slot, new=False)

    def _get_master_by_id(self, node_id):
        node = self._get_node_by_id(node_id)
        if not node:
            raise NotExistNode(f"{node_id}: {node}")
        elif not node.is_master():
            raise IsNotMasterNode()
        return node

    def _show_reshard_table(self, table):
        for source, slot in table:
            print(f"    Moving slot {slot} from {source.node_id}")

    def _compute_reshard_table(self, sources, num_slots):
        moved = []

        # Sort from bigger to smaller instance, for two reasons:
        # 1) If we take less slots than instances it is better to start
        #    getting from the biggest instances.
        # 2) We take one slot more from the first instance in the case of not
        #    perfect divisibility. Like we have 3 nodes and need to get 10
        #    slots, we take 4 from the first, and 3 from the rest. So the
        #    biggest is always the first.
        sorted_sources = sorted(sources, key=lambda n: len(n.slots))
        num_sources_slots = sum(map(lambda n: len(n.slots), sources))

        for i, s in enumerate(sorted_sources):
            # Every node will provide a number of slots proportional to the
            # slots it has assigned.
            num_move_slots = (float(num_slots) / num_sources_slots) * len(s.slots)
            num_move_slots = floor(num_move_slots) \
                                 if i != 0 else ceil(num_move_slots)
            moved += [(s, slot)
                       for slot in list(s.slots.keys())[:num_move_slots]]

        return moved

