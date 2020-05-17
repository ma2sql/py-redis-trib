'''
    def load_cluster_info_from_node(nodeaddr)
        node = ClusterNode.new(nodeaddr)
        node.connect(:abort => true)
        node.assert_cluster
        node.load_info(:getfriends => true)
        add_node(node)
        node.friends.each{|f|
            next if f[:flags].index("noaddr") ||
                    f[:flags].index("disconnected") ||
                    f[:flags].index("fail")
            fnode = ClusterNode.new(f[:addr])
            fnode.connect()
            next if !fnode.r
            begin
                fnode.load_info()
                add_node(fnode)
            rescue => e
                xputs "[ERR] Unable to load info for node #{fnode}"
            end
        }
        populate_nodes_replicas_info
    end
'''
from more_itertools import first_true
from .factory import NodesFactory
from .util import xprint
from .cluster_node import ClusterNodes

class ShowClusterInfo:
    def __init__(self, nodes):
        self._nodes = ClusterNodes(nodes)

    def load_cluster_info_from_node(self):
        self._populate_nodes_replicas_info()
        self._show_cluster_info()


    def _show_cluster_info(self):
        masters = 0
        keys = 0
        for n in self._nodes:
            if 'master' in n.flags:
                print(f"{n} ({n.node_id[:8]}...) -> {n.r.dbsize()} keys | {len(n.slots)} slots | {len(n.replicas)} slaves.")
                masters += 1
                keys += n.r.dbsize()
        xprint(f"[OK] {keys} keys in {masters} masters.")
        print(f"{keys/16384.0:.2f} keys per slot on average.")


    def _populate_nodes_replicas_info(self):
        for n in self._nodes:
            if n.is_slave():
                master = self._nodes.get_master(n)
                if master:
                    master.add_replica(n)

        
