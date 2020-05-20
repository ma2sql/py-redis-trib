from ..util import xprint

class ClusterNodesError(Exception): pass

class DelNode:
    
    __slots__ = ()

    def delete(self, del_node_id, rename_commands):
        try:
            deleting_node = self._get_node_by_id(del_node_id)
            if not deleting_node:
                raise ClusterNodesError(f"No such node ID {del_node_id}")
            if not deleting_node.is_deletable():
                raise ClusterNodesError(f"Node {deleting_node} is not empty! "\
                                        f"Reshard data away and try again.")

            self._forget_node(deleting_node)

            xprint(">>> SHUTDOWN the node.")
            self._shutdown_node(deleting_node, rename_commands)

        except ClusterNodesError as e:
            xprint(f"[ERR] {e}")

    def _forget_node(self, deleting_node):
        for node in self._nodes:
            if node == deleting_node:
                continue

            if node.is_my_master(deleting_node):
                self._replicate_another_master()

            node.cluster_forget(deleting_node.node_id)

    def _replicate_another_master(self, replica): 
        master = self._nodes.get_master_with_least_replicas()
        xprint(f">>> {node} as replica of {master}")
        node.cluster_replicate(master.node_id)

    def _shutdown_node(self, deleting_node, rename_commands):
        try:
            deleting_node.shutdown(rename_commands)
        except BaseException as e:
            xprint(f"[ERR] {e}") 
        else:
            xprint(f"[OK] SHUTDOWN is complete")

