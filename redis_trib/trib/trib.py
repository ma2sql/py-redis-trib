from ..util import xprint
from ..const import CLUSTER_HASH_SLOTS
import time

from .check_cluster import CheckCluster
from .create_cluster import CreateCluster
from .show_cluster import ShowCluster

class RedisTrib:
    def __init__(self, nodes):
        self._nodes = nodes

    def create(self, user_custom, replicas=0):
        create_cluster = CreateCluster(self._nodes, user_custom, replicas)
        create_cluster.create()

    def show(self):
        show_cluster = ShowCluster(self._nodes)
        show_cluster.show()

    def check(self):
        check_cluster = CheckCluster(self._nodes)
        check_cluster.check()

    def add(self, new_node, is_slave, master_id):
        self.check()
        add_node = AddNode(self._nodes, new_node, is_slave, master_id)
        add_node.add()

