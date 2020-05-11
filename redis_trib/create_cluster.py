import itertools
import more_itertools
import abc
import random
import time

from .const import CLUSTER_HASH_SLOTS
from .util import xprint, group_by
from .affinity_score import (
    get_anti_affinity_score,
    evaluate_anti_affinity
)
from .cluster_node import ClusterNode
from .exceptions import UnassignedNodesRemain
from .consistency import wait_cluster_join


class CreateCluster:
    def __init__(self, role_distribution):
        self._nodes = None
        self._masters = None
        self._role_distribution = role_distribution
    
    def create(self):
        xprint(">>> Creating cluster")
        self._role_distribution.distribute()

        self._nodes = self._role_distribution.nodes
        self._masters = [n for n in self._nodes if not n.replicate]

        evaluate_anti_affinity(self._nodes)

        xprint(f">>> Performing hash slots allocation "
               f"on {len(self._masters)} nodes...")
        self._alloc_slots()
        self._show_nodes()

        # yes_or_die "Can I set the above configuration?"
        self._flush_nodes_config()

        xprint(">>> Nodes configuration updated")
        xprint(">>> Assign a different config epoch to each node")
        self._assign_config_epoch()

        xprint(">>> Sending CLUSTER MEET messages to join the cluster")
        self._join_cluster()

        time.sleep(1)
        wait_cluster_join(self._nodes)
        self._flush_nodes_config()

    def _show_nodes(self):
        for n in self._nodes:
            print(n.info_string())

    def _get_role_distribution_strategy(self):
        if more_itertools.first_true(self._nodes, pred=lambda n: n.master_addr):
            return CustomRoleDistribution(self._nodes)
        return OriginalRoleDistribution(self._nodes)

    def _alloc_slots(self):
        slots_per_node = float(CLUSTER_HASH_SLOTS) / len(self._masters)
        first = 0
        cursor = 0.0
        for i, m in enumerate(self._masters):
            last = round(cursor + slots_per_node - 1)
            if last > CLUSTER_HASH_SLOTS or i == len(self._masters) - 1:
                last = CLUSTER_HASH_SLOTS - 1
            if last < first:
                last = first
            m.add_slots(list(range(first, last+1)))
            first = last+1
            cursor += slots_per_node

    def _flush_nodes_config(self):
        for n in self._nodes:
            n.flush_node_config()

    def _assign_config_epoch(self):
        for config_epoch, m in enumerate(self._masters, 1):
            xprint(f"[WARNING] {m}: {config_epoch}")
            m.set_config_epoch(config_epoch)

    def _join_cluster(self):
        first = self._nodes[0]
        for n in self._nodes[1:]:
            n.cluster_meet(first.host, first.port)


class RoleDistribution(abc.ABC):
    def __init__(self, nodes):
        self._nodes = nodes

    @property
    def nodes(self):
        return self._nodes

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
        super().__init__(nodes)
        self._replicas = replicas
        self._masters = None
        self._interleaved = []

    def _check_create_parameters(self):
        if (len(self.nodes) / (self._replicas + 1) < 3):
            xprint(f"""*** ERROR: Invalid configuration for cluster creation.\n"""
                   f"""*** Redis Cluster requires at least 3 master nodes.\n"""
                   f"""*** This is not possible with {len(self.nodes)} """
                   f"""nodes and {self._replicas} replicas per node.\n"""
                   f"""*** At least {3*(self._replicas+1)} nodes are required.\n""")
            raise CreateClusterException('Invalid configuration for cluster creation')

    def distribute(self):
        self._interleaved_nodes()
        self._set_replicas_every_master()
        self._optimize_anti_affinity()

    def _interleaved_nodes(self):
        host_to_node = group_by(self.nodes, key=lambda n: n.host)
        interleaved = list(itertools.chain(*itertools.zip_longest(*host_to_node.values())))
        master_count = int(len(self.nodes) / (self._replicas+1))
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

                    if assignment_verbose:
                        if assign == OriginalRoleDistribution._REQUESTED:
                            print(f"Requesting total of {self._replicas} replicas "\
                                  f"({assigned_replicas} replicas assigned "\
                                  f"so far with {len(self._interleaved)} total remaining).")
                        elif assign == OriginalRoleDistribution._UNUSED:
                            print(f"Assigning extra instance to replication "\
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
                    print(f"Adding replica {slave} to {m}")

                    # If we are in the "assign extra nodes" loop,
                    # we want to assign one extra replica to each
                    # master before repeating masters.
                    # This break lets us assign extra replicas to masters
                    # in a round-robin way.
                    if assign == OriginalRoleDistribution._UNUSED:
                        break
            
        if self._interleaved:
            xprint(f"[ERROR] {self._interleaved}")
            raise UnassignedNodesRemain(f"Unassigned nodes remain: {len(self._interleaved)}")


    def _optimize_anti_affinity(self):
        print(">>> Trying to optimize slaves allocation for anti-affinity")

        # Effort is proportional to cluster size...
        maxiter = 500 * len(self.nodes) 
   
        score, offenders = get_anti_affinity_score(self.nodes)
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

            new_score, new_offenders = get_anti_affinity_score(self.nodes)
            # If the change actually makes thing worse, revert. Otherwise
            # leave as it is becuase the best solution may need a few
            # combined swaps.
            if new_score > score:
                first.set_as_replica(first.replicate)
                second.set_as_replica(second.replicate)
            else:
                score = new_score
                offenders = new_offenders


class CustomRoleDistribution(RoleDistribution):
    def __init__(self, nodes):
        super().__init__(nodes)

    def _check_create_parameters(self):
        master_nodes = [n for n in self.nodes if not n.master_addr]
        if len(master_nodes) < 3:
            xprint(f"""*** ERROR: Invalid configuration for cluster creation.\n"""
                   f"""*** Redis Cluster requires at least 3 master nodes.\n"""
                   f"""*** This is not possible with {len(master_nodes)} nodes.\n"""
                   f"""*** At least 3 master nodes are required.\n""")
            raise CreateClusterException('Invalid configuration for cluster creation')

    def distribute(self):
        self._set_replication()

    def _set_replication(self):
        for n in self.nodes:
            if n.master_addr:
                master = first_true(self.nodes, pred=lambda m: m.addr == n.master_addr) 
                n.set_as_replica(master.node_id)

