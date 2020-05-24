from .mixins import (
    Common, CreateCluster, CheckCluster, ReshardCluster,
    ShowCluster, AddNode, DelNode
)


class RedisTrib(Common, CreateCluster, CheckCluster, ReshardCluster,
                ShowCluster, AddNode, DelNode):

    def __init__(self, nodes, password=None):
        self._nodes = nodes
        self._password = password
        self._errors = []

