from .create_cluster import (
    CreateCluster,
    OriginalRoleDistribution,
    CustomRoleDistribution,
)
from .factory import NodesFactory
from .load_info import ShowClusterInfo
from .check_cluster import CheckCluster

def create_cluster_command(addrs, password, replicas, user_custom):
    nodes = NodesFactory.create_new_nodes(addrs, password)
    if user_custom:
        role_distribution = CustomRoleDistribution(nodes)
    else:
        role_distribution = OriginalRoleDistribution(nodes, replicas)
    CreateCluster(role_distribution).create()

def info_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    show_cluster = ShowClusterInfo(nodes)
    show_cluster.load_cluster_info_from_node()


def check_cluster_command(addr, password):
    nodes = NodesFactory.create_nodes_with_friends(addr, password)
    nodes.populate_nodes_replicas_info()
    nodes.show_nodes()
    check_cluster = CheckCluster(nodes)
    check_cluster.check_config_consistency()
    check_cluster.check_open_slots()
    check_cluster.check_slots_coverage()

