from .mixins import (
    Common, CreateCluster, CheckCluster,
    ShowCluster, AddNode, DelNode,
    MoveSlot, ReshardCluster, RebalanceCluster, FixCluster,
    CallCluster, ImportCluster
)


class RedisTrib(Common, CreateCluster, CheckCluster,
                MoveSlot, ReshardCluster, RebalanceCluster, FixCluster,
                ShowCluster, AddNode, DelNode, CallCluster, ImportCluster):

    def __init__(self, nodes, password=None, unreachable_masters=0):
        self._nodes = nodes
        self._password = password
        self._num_errors = 0
        self._unreachable_masters = 0

