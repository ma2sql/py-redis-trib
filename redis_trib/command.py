from .trib import RedisTrib
from .factory import NodeFactory, NodesFactory

def create_cluster_command(addrs, password, replicas, user_custom):
    nodes = NodesFactory.create_new_nodes(addrs, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.create(user_custom, replicas)

def info_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.show()

def check_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.check_cluster()

def add_node_command(addr, password, new_addr, is_slave, master_id):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    new_node = NodeFactory.create_empty_node(new_addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.add(new_node, is_slave, master_id)

