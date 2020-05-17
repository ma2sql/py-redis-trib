
class AddNode:
    def __init__(self, nodes, new_node, is_slave, master_id):
        self._nodes = nodes
        self._new_node = new_node
        self._is_slave = is_slave
        self._master_id = master_id

    def add_node(self):
        self._nodes.add_node(self._new_node)
        self._nodes.join_cluster(self._new_node)
        if self._is_slave:
            self._set_replication()

    def _get_master_node(self):
        master = None
        if self._is_slave:
            if self._master_id:
                master = self._nodes.get_node_by_name(self._master_id)
                if not master:
                    xprint(f"[ERR] No such master ID {self._master_id}")
            else:
                master = self._nodes.get_master_with_least_replicas()
                xprint(f"Automatically selected master {master}")

        return master
    
    def _set_replication(self):
        self._nodes.wait_cluster_join()
        master = self._get_master_node()
        xprint(f">>> Configure node as replica of {master}.")
        self._new_node.cluster_replicate(master.node_id)
