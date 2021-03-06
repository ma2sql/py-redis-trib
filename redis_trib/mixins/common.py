import time
from functools import reduce
from more_itertools import first_true

from ..util import xprint


class Common:
    
    __slots__ = ()

    def _show_nodes(self):
        for n in self._nodes:
            print(n.info_string())

    def _get_masters(self):
        return [n for n in self._nodes
                  if n.is_master()]

    def _get_node_by_id(self, node_id):
        return first_true(self._nodes, pred=lambda n: n.node_id == node_id)

    def _get_node_by_addr(self, addr):
        return first_true(self._nodes, pred=lambda n: n.addr == addr)

    def _get_node_by_abbreviated_id(self, abbreviated_id):
        '''
        Like get_node_by_name but the specified name can be just the first
        part of the node ID as long as the prefix in unique across the
        cluster.
        '''
        l = len(abbreviated_id)
        candidates = [n for n in self._nodes
                      if n.node_id[:l] == abbreviated_id]
        
        if len(candidates) != 1:
            raise CannotFindNode(f"Too many nodes were found, or not found: "\
                    f"abbreviated_id={abbreviated_id} found={candidates}")

        return candidates[0]

    def _get_master_with_least_replicas(self):
        sorted_masters = sorted(self._get_masters(),
                             key=lambda n: len(n.replicas))
        return sorted_masters[0] if sorted_masters else None

    def _is_config_consistent(self):
        return len(set(map(lambda n: n.get_config_signature(),
                       self._nodes))) == 1

    def _wait_cluster_join(self):
        print("Waiting for the cluster to join")
        while not self._is_config_consistent():
            print(".", end="", flush=True)
            time.sleep(1)
        print()

    def _get_opened_slots(self):
        for n in self._nodes: 
            yield n, n.migrating, n.importing

    def _add_node(self, node):
        self._nodes.append(node)

    def _join_cluster(self, meet_nodes):
        first = self._nodes[0]
        for n in meet_nodes:
            n.cluster_meet(first.host, first.port)

    def _replcate_master(self, master, replica):
        replica.cluster_replicate(master.node_id)

    def _increase_num_errors(self):
        self._num_errors += 1

    def _has_errors(self):
        return self._num_errors == 0

    def _get_slot_owners(self, slot):
        owners = []
        for n in self._get_masters():
            if n.slots.get(slot):
                owners.append(n)

        return owners

    def _get_covered_slots(self):
        return set(slot for n in self._nodes
                        for slot in n.slots.keys())
