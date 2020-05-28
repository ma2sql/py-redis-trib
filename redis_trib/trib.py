from .mixins import (
    Common, CreateCluster, CheckCluster,
    ShowCluster, AddNode, DelNode,
    MoveSlot, ReshardCluster, RebalanceCluster
)


class RedisTrib(Common, CreateCluster, CheckCluster,
                MoveSlot, ReshardCluster, RebalanceCluster, 
                ShowCluster, AddNode, DelNode):

    def __init__(self, nodes, password=None):
        self._nodes = nodes
        self._password = password
        self._errors = []

