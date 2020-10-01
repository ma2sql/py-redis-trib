from math import ceil, floor
from ..util import query_yes_no
import redis

class IsNotMasterNode(Exception): pass
class NotExistNode(Exception): pass



@contextmanager
def move_slot_context(slot, source, target, cold=False, update=False):
    if not cold:
        target.cluster_setslot_importing(slot, source)
        source.cluster_setslot_migrating(slot, target)

    yield

    if not cold:
        for n in self._get_masters():
            n.cluster_setslot_node(slot, target)

    if update:
        source.del_slots(slot)
        target.add_slots(slot, new=False)



class MoveSlot:

    __slots__ = ()

    # Move slots between source and target nodes using MIGRATE.
    #
    # Options:
    # :verbose -- Print a dot for every moved key.
    # :fix     -- We are moving in the context of a fix. Use REPLACE.
    # :cold    -- Move keys without opening slots / reconfiguring the nodes.
    # :update  -- Update nodes.info[:slots] for source/target nodes.
    # :quiet   -- Don't print info messages.
    def _move_slot(self, source, target, slot, pipeline=10, update=True, dot=False, cold=False, quiet=True, fix=False):
        # We start marking the slot as importing in the destination node,
        # and the slot as migrating in the target host. Note that the order of
        # the operations is important, as otherwise a client may be redirected
        # to the target node that does not yet know it is importing this slot.
        if not quiet:
            print(f"Moving slot {slot} from {source} to {target}: ", end="")
            
        if not cold:
            target.cluster_setslot_importing(slot, source)
            source.cluster_setslot_migrating(slot, target)

        timeout = 60
        # Migrate all the keys from source to target using the MIGRATE command
        while True:
            keys_in_slot = source.cluster_get_keys_in_slot(slot, pipeline)
            if len(keys_in_slot) == 0:
                break

            try:
                source.migrate(target.host, target.port, keys_in_slot, timeout,
                               auth=self._password)
            except redis.exceptions.ResponseError as e:
                if fix and str(e).find('BUSYKEY'):
                    xprint("*** Target key exists. Replacing it for FIX.")
                    source.migrate(target.host, target.port, keys_in_slot,
                        timeout, auth=self._password, replace=True)
                else:
                    xprint("")
                    xprint.error(f"Calling MIGRATE: {e}")

            if True:
                xprint("." * len(keys_in_slot))


        if not quiet:
            xprint()

        # Set the new node as the owner of the slot in all the known nodes.
        #if opt.get('cold') is not None:
        if True:
            for n in self._get_masters():
                n.cluster_setslot_node(slot, target)

        # Update the node logical config
        if update:
            source.del_slots(slot)
            target.add_slots(slot, new=False)


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


    def _compute_reshard_table_by_range(self, slots_range):
        moved = []
        for slot in slots_range:
            owners = self._get_slot_owners(slot)
            if len(owners) != 1:
                raise BaseException(f'Too many owners: {owners}') 
             
            moved.append((owners[0], slot))

        return moved 

