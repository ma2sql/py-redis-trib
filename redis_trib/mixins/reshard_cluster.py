from math import ceil, floor
from ..util import query_yes_no

class IsNotMasterNode(Exception): pass
class NotExistNode(Exception): pass

class ReshardCluster:

    __slots__ = ()

    def reshard_cluster(self, from_ids, to_id, pipeline, timeout, num_slots, slots_range, yes=False):
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

