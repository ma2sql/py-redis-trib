
class RedisTribException(Exception): pass

class AbortedByUserException(RedisTribException): pass

class NodeException(Exception): pass
class NodeConnectionException(NodeException): pass
class AssertNodeException(NodeException): pass
class LoadInfoFailureException(NodeException): pass

class TooSmallMastersError(RedisTribException): pass

class CreateClusterException(Exception): pass
class UnassignedNodesRemain(CreateClusterException): pass
