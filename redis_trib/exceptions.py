
class ClusterNodeException(Exception): pass
class CannotConnectToRedis(ClusterNodeException): pass

class ClusterNodeError(Exception): pass
class AssertClusterError(ClusterNodeError): pass
class AssertEmptyError(ClusterNodeError): pass

class RedisTribError(Exception): pass
class TooSmallMastersError(RedisTribError): pass

class CreateClusterException(Exception): pass
class UnassignedNodesRemain(CreateClusterException): pass
