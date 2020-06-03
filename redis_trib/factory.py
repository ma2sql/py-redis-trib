from .cluster_node import ClusterNode
from more_itertools import first_true


class NodeFactory:
    @classmethod
    def create_normal_node(cls, addr, password):
        node = ClusterNode(addr, password)
        node.connect(abort=True)
        node.assert_cluster()
        node.load_info()
        return node

    @classmethod
    def create_empty_node(cls, addr, password):
        node = ClusterNode(addr, password)
        node.connect(abort=True)
        node.assert_cluster()
        node.load_info()
        node.assert_empty()
        return node

    @classmethod
    def create_friend_node(cls, addr, password):
        node = ClusterNode(addr, password)
        node.connect(abort=False)
        try:
            node.load_info() 
            return node
        except redis.exceptions.ResponseError as e:
            xprint(f"[ERR] Unable to load info for node {fnode}: {e}")
        return None


class NodesFactory:
    @classmethod
    def create_new_nodes(cls, addrs, password):
        nodes = []
        for addr in addrs:
            master_addr, *slave_addrs = addr.split(',')
            master = NodeFactory.create_empty_node(master_addr, password) 
            nodes.append(master)
            for slave_addr in slave_addrs:
                slave = NodeFactory.create_empty_node(slave_addr, password) 
                slave.master_addr = master_addr
                nodes.append(slave)
        return nodes

    @classmethod
    def create_nodes_with_friends(cls, addr, password):
        nodes = []
        node = NodeFactory.create_normal_node(addr, password)
        nodes.append(node)

        for faddr, flags in node.friends:
            if set(flags) & set(['noaddr', 'disconnected', 'fail']):
                continue
            fnode = NodeFactory.create_friend_node(faddr, password)
            if fnode:
                nodes.append(fnode)

        cls._populate_nodes_replicas_info(nodes)

        return nodes

    @classmethod
    def _populate_nodes_replicas_info(cls, nodes):
        for n in nodes:
            if n.is_slave():
                master = first_true(nodes, pred=lambda m: m.is_my_replica(n))
                if not master:
                    # TODO: print warning message
                    continue
                master.add_replica(n)

