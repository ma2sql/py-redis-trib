from math import ceil, floor
from ..util import query_yes_no
import redis

class IsNotMasterNode(Exception): pass
class NotExistNode(Exception): pass


class MoveSlot:

    __slots__ = ()


    def _set_importing_and_migrating(self, source, target):
        target.cluster_setslot_importing(slot, source)
        source.cluster_setslot_migrating(slot, target)
   

    def _notify_new_owner(self, source, target):
        target.cluster_setslot_node(slot, target)
        source.cluster_setslot_node(slot, target)
        for n in [n for n in self._get_masters()
                    if n not in (source, target)]:
            n.cluster_setslot_node(slot, target)


    def _update_node_config(self, source, target):
        source.del_slots(slot)
        target.add_slots(slot, new=False)


    def _move_slot(self, source, target, slot, pipeline=10, timeout=60,
                         update=True, cold=False, quiet=True, fix=False):
        quiet_or_not = xprint.quiet_or_not(quiet)

        quiet_or_not(f"Moving slot {slot} from {source} to {target}: ", end="")

        if not cold:
           self._set_importing_and_migrating(source, target) 

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
   
            quiet_or_not("." * len(keys_in_slot))

        quiet_or_not()

        if not cold:
            self._notify_new_owner(source, target)

        if update:
            self._update_node_config(source, target)
    


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

