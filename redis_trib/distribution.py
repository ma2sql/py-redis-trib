import abc
import itertools
import collections
import more_itertools
from .exceptions import UnassignedNodesRemain
from .util import group_by

from .xprint import xprint


class RoleDistribution(abc.ABC):
    @abc.abstractmethod
    def distribute(self):
        pass

    @abc.abstractmethod
    def _check_create_parameters(self):
        pass


class OriginalRoleDistribution(RoleDistribution):
    
    _REQUESTED = 'REQUESTED'
    _UNUSED = 'UNUSED'

    def __init__(self, nodes, replicas=0):
        self._nodes = nodes
        self._replicas = replicas
        self._masters = None
        self._interleaved = []

    def distribute(self):
        self._check_create_parameters()
        self._interleaved_nodes()
        self._set_replicas_every_master()
        self._optimize_anti_affinity()

    def _check_create_parameters(self):
        if (len(self._nodes) / (self._replicas + 1) < 3):
            xprint(f"""*** ERROR: Invalid configuration for cluster creation.\n"""
                   f"""*** Redis Cluster requires at least 3 master nodes.\n"""
                   f"""*** This is not possible with {len(self._nodes)} """
                   f"""nodes and {self._replicas} replicas per node.\n"""
                   f"""*** At least {3*(self._replicas+1)} nodes are required.\n""")
            raise CreateClusterException('Invalid configuration for cluster creation')

    def _interleaved_nodes(self):
        host_to_node = group_by(self._nodes, key=lambda n: n.host)
        interleaved = list(itertools.chain(*itertools.zip_longest(*host_to_node.values())))
        master_count = int(len(self._nodes) / (self._replicas+1))
        self._masters = interleaved[:master_count]

        # Rotating the list sometimes helps to get better initial
        # anti-affinity before the optimizer runs.
        interleaved = interleaved[master_count:]
        self._interleaved = interleaved[:-1] + interleaved[-1:]

    def _set_replicas_every_master(self): 
        # Select N replicas for every master.
        # We try to split the replicas among all the IPs with spare nodes
        # trying to avoid the host where the master is running, if possible.
        #
        # Note we loop two times.  The first loop assigns the requested
        # number of replicas to each master.  The second loop assigns any
        # remaining instances as extra replicas to masters.  Some masters
        # may end up with more than their requested number of replicas, but
        # all nodes will be used.
        assignment_verbose = True

        for assign in [OriginalRoleDistribution._REQUESTED,
                       OriginalRoleDistribution._UNUSED]:
            for m in self._masters:
                assigned_replicas = 0
                for _ in range(self._replicas):
                    if len(self._interleaved) == 0:
                        break

                    if assign == OriginalRoleDistribution._REQUESTED:
                        xprint.verbose(f"Requesting total of {self._replicas} replicas "\
                                       f"({assigned_replicas} replicas assigned "\
                                       f"so far with {len(self._interleaved)} total remaining).")
                    elif assign == OriginalRoleDistribution._UNUSED:
                        xprint.verbose(f"Assigning extra instance to replication "\
                                       f"role too ({len(self._interleaved)} remaining).")

                    # Return the first node not matching our current master
                    node = more_itertools.first_true(self._interleaved,
                                                     pred=lambda n: n.host != m.host)

                    # If we found a node, use it as a best-first match.
                    # Otherwise, we didn't find a node on a different IP, so we
                    # go ahead and use a same-IP replica.
                    if node:
                        slave = node
                        self._interleaved = list(filter(lambda n: node != n, self._interleaved))
                    else:
                        slave = self._interleaved.pop(0)

                    slave.set_as_replica(m.node_id)
                    assigned_replicas += 1
                    xprint.verbose(f"Adding replica {slave} to {m}")

                    # If we are in the "assign extra nodes" loop,
                    # we want to assign one extra replica to each
                    # master before repeating masters.
                    # This break lets us assign extra replicas to masters
                    # in a round-robin way.
                    if assign == OriginalRoleDistribution._UNUSED:
                        break
            
        if self._interleaved:
            xprint.error(f"{self._interleaved}")
            raise UnassignedNodesRemain(f"Unassigned nodes remain: {len(self._interleaved)}")

    def _optimize_anti_affinity(self):
        xprint(">>> Trying to optimize slaves allocation for anti-affinity")

        # Effort is proportional to cluster size...
        maxiter = 500 * len(self._nodes) 
   
        score, offenders = self._get_anti_affinity_score()
        for _ in range(maxiter):
            # Optimal anti affinity reached
            if score == 0:
                break

            # We'll try to randomly swap a slave's assigned master causing
            # an affinity problem with another random slave, to see if we
            # can improve the affinity.
            first = random.choice(offenders)
            nodes = list(filter(lambda n: n != first and n.replicate, nodes))
            if len(nodes) == 0:
                break

            second = random.choice(nodes)

            first.set_as_replica(second.replicate)
            second.set_as_replica(first.replicate)

            new_score, new_offenders = self._get_anti_affinity_score()
            # If the change actually makes thing worse, revert. Otherwise
            # leave as it is becuase the best solution may need a few
            # combined swaps.
            if new_score > score:
                first.set_as_replica(first.replicate)
                second.set_as_replica(second.replicate)
            else:
                score = new_score
                offenders = new_offenders

    def _get_anti_affinity_score(self):
        score = 0
        # List of offending slaves to return to the caller
        offending = []
        # First, split nodes by host
        host_to_node = group_by(self._nodes, key=lambda n: n.host)
    
        # Then, for each set of nodes in the same host, split by
        # related nodes (masters and slaves which are involved in
        # replication of each other)
        for host, nodes in host_to_node.items():
            related = collections.defaultdict(list)
            for n in nodes:
                if n.replicate:
                    related[n.replicate].append('s')
                else:
                    related[n.node_id].append('m')
    
            # Now it's trivial to check, for each related group having the
            # same host, what is their local score.
            for node_id, types in related.items():
                if len(types) < 2:
                    continue
                # Make sure :m if the first if any
                sorted_types = sorted(types)
                if sorted_types[0] == 'm':
                    score += 10000 * (len(types) -1)
                else:
                    score += 1 * len(types)
    
                # Populate the list of offending node
                for n in nodes:
                    if n.replicate == node_id and n.host == host:
                        offending.append(n)
        
        return score, offending
    
    def _evaluate_anti_affinity(self):
        score, *_ = self._get_anti_affinity_score()
        if score == 0:
            xprint.ok("Perfect anti-affinity obtained!")
        elif score >= 10000:
            xprint.warning("Some slaves are in the same host as their master")
        else:
            xprint.warning("Some slaves of the same master are in the same host")
 

class CustomRoleDistribution(RoleDistribution):
    def __init__(self, nodes, replicas=None):
        self._nodes = nodes

    def distribute(self):
        self._check_create_parameters()
        self._set_replication()

    def _check_create_parameters(self):
        master_nodes = [n for n in self._nodes if not n.master_addr]
        if len(master_nodes) < 3:
            xprint.error(f"""*** ERROR: Invalid configuration for cluster creation.\n"""
                         f"""*** Redis Cluster requires at least 3 master nodes.\n"""
                         f"""*** This is not possible with {len(master_nodes)} nodes.\n"""
                         f"""*** At least 3 master nodes are required.\n""")
            raise CreateClusterException('Invalid configuration for cluster creation')

    def _set_replication(self):
        for n in self._nodes:
            if n.master_addr:
                master = first_true(self._nodes, pred=lambda m: m.addr == n.master_addr) 
                n.set_as_replica(master.node_id)

