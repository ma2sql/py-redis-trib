from .mixins import (
    Common, CreateCluster, CheckCluster,
    ShowCluster, AddNode, DelNode
)


class RedisTrib(Common, CreateCluster, CheckCluster,
                ShowCluster, AddNode, DelNode):

    def __init__(self, nodes):
        self._nodes = nodes

