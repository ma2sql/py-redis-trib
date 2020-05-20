
class AddNode:

    __slots__ = ()

    def add(self, new_node, is_slave=False, master_addr=None, master_id=None):
        self._add_node(new_node)
        self._join_cluster([new_node])
        if self._is_slave:
            master = self._get_master_nodes(master_id, master_addr) 
            self._set_replication(master, new_node)

    def _wait_and_replicate_master(self, master, new_node):
        self._wait_cluster_join()
        xprint(f">>> Configure node as replica of {master}.")
        self._replcate_master(master, new_node)

    def _get_master_node(self, master_addr, master_id):
        master = None
        if master_id:
            master = self._get_node_by_id(master_id)
        elif master_addr:
            master = self._get_node_by_addr(master_addr)

        if not master:
            xprint(f"[ERR] No such master id={master_id}, addr={master_addr}")
            master = self._get_master_with_least_replicas()
            xprint(f"Automatically selected master {master}")

        return master
    
