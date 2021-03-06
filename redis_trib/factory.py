from .cluster_node import Node
from .exceptions import (
    NodeConnectionException,
    LoadInfoFailureException,
)
from .xprint import xprint
from more_itertools import first_true


class NodeFactory:
    @classmethod
    def create_normal_node(cls, addr, password):
        node = Node(addr, password)
        node.connect()
        node.assert_cluster()
        node.load_info()
        return node

    @classmethod
    def create_empty_node(cls, addr, password):
        node = NodeFactory.create_normal_node(addr, password)
        node.assert_empty()
        return node

    @classmethod
    def create_friend_node(cls, addr, password):
        node = Node(addr, password)
        try:
            node.connect()
            node.load_info() 
            return node
        except NodeConnectionException as e:
            xprint.error(e)
        except LoadInfoFailureException as e:
            xprint.error(f"Unable to load info for node {fnode}: {e}")
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
        unreachable_masters = 0

        node = NodeFactory.create_normal_node(addr, password)
        nodes.append(node)

        for faddr, flags in node.friends:
            if set(flags) & set(['noaddr', 'disconnected', 'fail']):
                continue
            fnode = NodeFactory.create_friend_node(faddr, password)
            if fnode:
                nodes.append(fnode)
            elif 'master' in flags:
                unreachable_masters += 1


        cls._populate_nodes_replicas_info(nodes)

        return nodes, unreachable_masters

    @classmethod
    def _populate_nodes_replicas_info(cls, nodes):
        for n in nodes:
            if n.is_slave():
                master = first_true(nodes, pred=lambda m: m.is_my_replica(n))
                if not master:
                    # TODO: print warning message
                    continue
                master.add_replica(n)

