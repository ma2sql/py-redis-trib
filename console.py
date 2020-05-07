import redis_trib

if __name__ == '__main__':
    node_addrs = [
    ]
    print(node_addrs)
    password = None
    nodes_factory = redis_trib.NodesFactory(node_addrs, password)
    nodes = nodes_factory.create_nodes()
    create_cluster = redis_trib.CreateCluster(nodes, redis_trib.OriginalRoleDistribution(nodes, replicas=1), replicas=1) 
    create_cluster.create()
