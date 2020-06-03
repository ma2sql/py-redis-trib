from .trib import RedisTrib
from .distribution import OriginalRoleDistribution, CustomRoleDistribution  
from .factory import NodeFactory, NodesFactory

def create_cluster_command(addrs, password, replicas, user_custom):
    nodes = NodesFactory.create_new_nodes(addrs, password)
    redis_trib = RedisTrib(nodes)
    if user_custom:
        role_distribution = CustomRoleDistribution(nodes)
    else:
        role_distribution = OriginalRoleDistribution(nodes, replicas)
    redis_trib.create_cluster(role_distribution)


def info_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.show()


def check_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.check()


def add_node_command(addr, new_addr, password, is_slave, master_id, addr_as_master):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    master_addr = addr if addr_as_master else None
    if master_id or master_addr:
        is_slave = True
    new_node = NodeFactory.create_empty_node(new_addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.add(new_node, is_slave, master_id, master_addr)


def delete_node_command(addr, del_node_id, password, rename_commands):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes)
    redis_trib.delete(del_node_id, rename_commands)


def reshard_cluster_command(addr, password, from_ids, to_id,
        pipeline, timeout, num_slots, yes):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes, password)
    redis_trib.check()
    redis_trib.reshard_cluster(from_ids, to_id, pipeline, timeout, num_slots, yes=False)


def rebalance_cluster_command(addr, password, weights, use_empty_masters,
        pipeline, timeout, threshold, simulate):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes, password)
    redis_trib.check()
    redis_trib.rebalance_cluster(weights, use_empty_masters, pipeline, timeout, threshold, simulate)


def fix_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes, password)
    redis_trib.check()
    redis_trib.fix()


def call_cluster_command(addr, password, *command):
    import sys, os
    old_stdout = sys.stdout
    try:
        f = open(os.devnull, 'w')
        sys.stdout = f

        nodes = NodesFactory.create_nodes_with_friends(addr, password)
        redis_trib = RedisTrib(nodes, password)
        redis_trib.check(quiet=True)
    finally:
        if f: f.close()
        sys.stdout = old_stdout

    redis_trib.call(*command)


def import_cluster_command(addr, password, from_addr, from_password, replace, copy):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    redis_trib = RedisTrib(nodes, password)
    redis_trib.check()
    redis_trib.import_cluster(from_addr, from_password, replace, copy)

