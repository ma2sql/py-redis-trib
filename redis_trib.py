import redis
import types
import collections
from itertools import (
    zip_longest, chain, groupby
)
from more_itertools import (
    locate, first_true
)
from abc import ABC, abstractmethod
import random
import time

from src.util import (
    xprint,
    group_by
)
from src.affinity_score import (
    get_anti_affinity_score,
    evaluate_anti_affinity
)

CLUSTER_HASH_SLOTS = 16384


class ClusterNodeException(Exception): pass
class CannotConnectToRedis(ClusterNodeException): pass


class ClusterNode:
    def __init__(self, addr, password=None, master_addr=None):
        s = addr.split('@')[0].split(':')
        if len(s) < 2:
            print(f"Invalid IP or Port (given as {addr}) - use IP:Port format")
            sys.exit(1)
        
        self._host = ':'.join(s[:-1])
        self._port = s[-1]
        self._password = password
        self._master_addr = master_addr
        self._node_id = None
        self._slots = {}
        self._migrating = []
        self._importing = []
        self._replicate = None
        self._replicas = []
        self._dirty = False
        self._r = None
        self._friends = []
        self._cluster_nodes = None

    def __eq__(self, obj):
        return self.addr == obj.addr

    def __str__(self):
        return self.addr

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def addr(self):
        return f"{self.host}:{self.port}"

    @property
    def node_id(self):
        return self._node_id

    @property
    def slots(self):
        return self._slots

    @property
    def master_addr(self):
        return self._master_addr
    
    @property
    def replicate(self):
        return self._replicate

    @master_addr.setter
    def master_addr(self, master_addr):
        self._master_addr = master_addr

    def connect(self, abort=False):
        if self._r:
            return
        print(f"Connecting to node {self}: ", end="")
        try:
            self._r = redis.StrictRedis(self.host, self.port,
                                        password=self._password,
                                        socket_timeout=60)
            self._r.ping()
        except redis.exceptions.ConnectionError:
            xprint(f"[ERR] Sorry, can't connect to node '{self}'")
            if abort:
                raise CannotConnectToRedis(f"[ERR] Sorry, can't connect to node '{self}'")
            self._r = None
        xprint("OK")

    def assert_cluster(self):
        cluster_enabled = self._r.info().get('cluster_enabled') or 0
        if int(cluster_enabled) != 1:
            raise AssertClusterError(f"[ERR] Node {self} is not configured as a cluster node.")

    def assert_empty(self):
        cluster_known_nodes = self._r.cluster('INFO').get('cluster_known_nodes') or 0
        cluster_known_nodes = int(cluster_known_nodes)
        keyspace = self._r.info().get('db0')

        if (keyspace or cluster_known_nodes != 1):
            raise AssertEmptyError(f"[ERR] Node {self} is not empty. "
                                   f"Either the node already knows other nodes (check with CLUSTER NODES)"
                                   f" or contains some key in database 0.")

    def load_info(self):
        for n in self.cluster_nodes.values():
            flags = self._parse_flags(n.get('flags'))
            if 'myself' in flags:
                self._node_id = n['node_id']
                self._flags = flags
                self._replicate = n['master_id'] != '-' and n['master_id']
                (slots, migrating, importing) = self._parse_slots(n['slots'])
                self.add_slots(slots)
                self._migrating = migrating
                self._importing = importing
                self._dirty = False
                break

    def _parse_flags(self, flags):
        return flags.split(',')

    @property
    def cluster_nodes(self):
        self._cluster_nodes = self._r.cluster('NODES')
        return self._cluster_nodes

    @property
    def friends(self):
        for addr, n in  self.cluster_nodes.items():
            flags = self._parse_flags(n['flags'])
            if 'myself' not in flags:
                yield addr, flags

    def _parse_slots(self, slots):
        parsed_slots = []
        migrating = {}
        importing = {}
        for s in slots:
            # ["[5461", ">", "b35b55daf26e84acdf17fed30d46ca97ccdd9169]"]
            if len(s) == 3:
                slot, direction, target = s
                slot = int(slot[1:])
                target = target[:-1]
                if direction == '>':
                    migrating[slot] = target
                elif direction == '<':
                    importing[slot] = target
            # ["0", "5460"]
            elif len(s) == 2:
                start = int(s[0])
                end = int(s[1]) + 1
                parsed_slots += list(range(start, end))
            # ["5462"]
            else:
                parsed_slots += [int(s[0])]

        return parsed_slots, migrating, importing


    def add_slots(self, slots):
        if isinstance(slots, (list, tuple, range)):
            self._slots.update({s: False for s in slots})
        else:
            self._slots.update({slots: False})
        self._dirty = True

    def set_as_replica(self, master_id):
        self._replicate = master_id
        self._dirty = True


    def flush_node_config(self):
        xprint(f"[WARN] {self}: {self._dirty}")
        if not self._dirty: return

        if self._replicate:
            try:
                self._r.cluster("REPLICATE", self._replicate)
            except redis.exceptions.ResponseError as e:
                xprint(f"[ERROR] {self._replicate} {e}")
                return
        else:
            _slots = {s: True for s, assigned in self._slots.items()
                              if not assigned}
            self._slots.update(_slots)
            self._r.cluster("ADDSLOTS", *_slots.keys())

        self._dirty = False
           
    def _summarize_slots(self, slots):
        _temp_slots = []
        for slot in sorted(slots):
            if not _temp_slots or _temp_slots[-1][-1] != (slot-1): 
                _temp_slots.append([])
            _temp_slots[-1][1:] = [slot]
        return ','.join(map(lambda slot_exp: '-'.join(map(str, slot_exp)), _temp_slots)) 
   
    def info_string(self):
        role = "M" if "master" in self._flags else "S" 
        info_str = ""
        if self._replicate and self._dirty:
            info_str = f"S: {self._replicate} {self}"
        else:
            info_str = f"{role}: {self._node_id} {self}\n"\
                       f"    slots:{self._summarize_slots(self._slots)} ({len(self._slots)}) "\
                       f"{','.join(filter(lambda flag: flag != 'myself', self._flags))}"
        if self._replicate:
            info_str += f"\n    replicates {self._replicate}"
        elif "master" in self._flags and self._replicas:
            info_str += f"\n    {len(self._replicas)} additional replica(s)"
        
        return info_str

    def assign_config_epoch(self, config_epoch):
        pass

    def cluster_meet(self, host, port):
        self._r.cluster('MEET', host, port)

    def get_config_signature(self):
        # Return a single string representing nodes and associated slots.
        # TODO: remove slaves from config when slaves will be handled
        # by Redis Cluster.
        config = []
        for n in self.cluster_nodes.values():
            slots, *_ = self._parse_slots(n['slots'])
            if 'master' not in self._parse_flags(n['flags']):
                continue
            config.append(f"{n['node_id']}:{self._summarize_slots(slots)}")
        return '|'.join(sorted(config))


class ClusterNodeError(Exception): pass
class AssertClusterError(ClusterNodeError): pass
class AssertEmptyError(ClusterNodeError): pass
class RedisTribError(Exception): pass
class TooSmallMastersError(RedisTribError): pass

class RedisTrib:
    def __init__(self, node_addrs):
        self._node_addrs = node_addrs
        self._nodes = []
        self._masters = []

    def _check_parameters(self):
        if len(self._masters) < 3:
            raise TooSmallMastersError('ERROR: Invalid configuration for cluster creation')

    def _add_node(self, node):
        self._nodes.append(node)

    def _connect_to_nodes(self):
        for n in self._nodes:
            n.connect()

    def _distribute_roles(self):
        for addr in self._node_addrs:
            master_addr, *slave_addrs = addr.split(',')
            master = ClusterNode(master_addr)
            self._nodes.append(master)
            self._masters.append(master)
            for slave_addr in slave_addrs:
                slave = ClusterNode(slave_addr)
                slave.master_addr = master_addr
                self._nodes.append(slave)

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
        config_epoch = 1
        for n in self._masters:
            try:
                #n.r.cluster('SET-CONFIG-EPOCH', config_epoch)
                n.assign_config_epoch(config_epoch)
            except BaseException:
                pass

    def _join_cluster(self):
        first = self._nodes[0]
        for n in self._nodes[1:]:
            n.cluster_meet(first.host, first.port)

    def _is_config_consistent(self):
        for n in self._nodes:
            n.get_config_signature()

    def _wait_cluster_join(self):
        pass

    def create_node(self, addr, master_addr=None, password=None):
        node = ClusterNode(addr, master_addr, password)
        node.connect(abort=True)
        node.assert_cluster()
        node.load_info()
        node.assert_empty()
        return node 


class CreateCluster:
    def __init__(self, node_addrs, replicas=0, password=None):
        self._node_addrs = node_addrs
        self._password = password
        self._nodes = []
        self._replicas = replicas
    
    def create(self):
        '''
        xputs ">>> Creating cluster"
        argv[0..-1].each{|n|
            node = ClusterNode.new(n)
            node.connect(:abort => true)
            node.assert_cluster
            node.load_info
            node.assert_empty
            add_node(node)
        }
        check_create_parameters
        xputs ">>> Performing hash slots allocation on #{@nodes.length} nodes..."
        alloc_slots
        show_nodes
        yes_or_die "Can I set the above configuration?"
        flush_nodes_config
        xputs ">>> Nodes configuration updated"
        xputs ">>> Assign a different config epoch to each node"
        assign_config_epoch
        xputs ">>> Sending CLUSTER MEET messages to join the cluster"
        join_cluster
        # Give one second for the join to start, in order to avoid that
        # wait_cluster_join will find all the nodes agree about the config as
        # they are still empty with unassigned slots.
        sleep 1
        wait_cluster_join
        flush_nodes_config # Useful for the replicas
        # Reset the node information, so that when the
        # final summary is listed in check_cluster about the newly created cluster
        # all the nodes would get properly listed as slaves or masters
        reset_nodes
        load_cluster_info_from_node(argv[0])
        check_cluster
        '''
        xprint(">>> Creating cluster")
        nodes_factory = NodesFactory(self._node_addrs, self._password)
        self._nodes = nodes_factory.create_nodes()

        self._check_create_parameters()
        
        # TODO: 이 지점에서 마스터와 슬레이브를 다시 분배함
        # 적절한 전략을 찾기 위해서 팩토리 메서드를 사용할 것
        role_distribution_strategy = self._get_role_distribution_strategy()
        role_distribution_strategy.create()
        self._masters = [n for n in self._nodes if not n.replicate]

        # TODO: anti affinitiy score를 이곳에서 계산?
        # 하지만, original 마스터/슬레이브 롤 분배를 분리하면, 그 오브젝트에도 필요한걸...
        # 그냥 심플하게, 별도의 모듈로 분리한다!
        evaluate_anti_affinity(self._nodes)

        xprint(f">>> Performing hash slots allocation "
               f"on {len(self._masters)} nodes...")
        self._alloc_slots()
        self._show_nodes()
        self._flush_nodes_config()
        self._join_cluster()
        wait_cluster_join(self._nodes)
        self._flush_nodes_config()

    def _check_create_parameters(self):
        if (len(self._nodes) / (self._replicas + 1) < 3
            or len([n for n in self._nodes if not n.master_addr]) < 3):
            print(f"""*** ERROR: Invalid configuration for cluster creation.\n"""
                  f"""*** Redis Cluster requires at least 3 master nodes.\n"""
                  f"""*** This is not possible with {len(self._nodes)} nodes and {self._replicas} replicas per node.\n"""
                  f"""*** At least {3*(self._replicas+1)} nodes are required.\n""")
            raise TooSmallMastersError('ERROR: Invalid configuration for cluster creation')

    def _show_nodes(self):
        for n in self._nodes:
            print(n.info_string())

    def _get_role_distribution_strategy(self):
        if first_true(self._nodes, pred=lambda n: n.master_addr):
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

    def _join_cluster(self):
        first = self._nodes[0]
        for n in self._nodes[1:]:
            n.cluster_meet(first.host, first.port)


def is_config_consistent(nodes):
    signatures=[]
    for n in nodes:
        signatures.append(n.get_config_signature())
    return len(set(signatures)) == 1

def wait_cluster_join(nodes):
    print("Waiting for the cluster to join")
    while not is_config_consistent(nodes):
        print(".", end="", flush=True)
        time.sleep(1)
    print()


class CreateClusterException(Exception): pass
class UnassignedNodesRemain(CreateClusterException): pass

class NodesFactory:
    def __init__(self, node_addrs, password):
        self._node_addrs = node_addrs
        self._password = password

    def create_nodes(self):
        nodes = []
        for addr in self._node_addrs:
            master_addr, *slave_addrs = addr.split(',')
            master = self._create_node(master_addr)
            nodes.append(master)
            for slave_addr in slave_addrs:
                slave = self._create_node(slave_addr)
                slave.master_addr = master_addr
                nodes.append(slave)
        return nodes

    def _create_node(self, addr):
        node = ClusterNode(addr, self._password)
        node.connect(abort=True)
        node.assert_cluster()
        node.load_info()
        node.assert_empty()
        return node 


class RoleDistribution(ABC):
    @abstractmethod
    def create(self):
        pass


class OriginalRoleDistribution(RoleDistribution):
    
    _REQUESTED = 'REQUESTED'
    _UNUSED = 'UNUSED'

    def __init__(self, nodes, replicas=0):
        super().__init__()
        self._nodes = nodes
        self._replicas = replicas
        self._masters = None
        self._interleaved = []

    def create(self):
        self._interleaved_nodes()
        self._set_replicas_every_master()
        self._optimize_anti_affinity()
        return self._nodes

    def _interleaved_nodes(self):
        host_to_node = group_by(self._nodes, key=lambda n: n.host)
        interleaved = list(chain(*zip_longest(*host_to_node.values())))
        master_count = int(len(self._nodes) / (self._replicas+1))
        self._masters = interleaved[:masters_count]
        # Rotating the list sometimes helps to get better initial
        # anti-affinity before the optimizer runs.
        self._interleaved = interleaved[master_count:-1] + interleaved[-1:]

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

        for assign in [OriginalCreateNodesFactory,_REQUESTED,
                       OriginalCreateNodesFactory._UNUSED]:
            for m in self._masters:
                assigned_replicas = 0
                for _ in range(self._replicas):
                    if len(self._interleaved) == 0:
                        break

                    if assignment_verbose:
                        if assign == OriginalCreateNodesFactory._REQUESTED:
                            print(f"Requesting total of {self._replicas} replicas "\
                                  f"({self._assigned_replicas} replicas assigned "\
                                  f"so far with {len(self._interleaved)} total remaining).")
                        elif assign == OriginalCreateNodesFactory._UNUSED:
                            print(f"Assigning extra instance to replication "\
                                  f"role too ({len(self._interleaved)} remaining).")

                    # Return the first node not matching our current master
                    node = first_true(self._interleaved, pred=lambda n: n.host != m.host)

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
                    if assign == UNUSED:
                        break
            
        if self._interleaved:
            raise UnassignedNodesRemain('Unassigned nodes remain')


    def _optimize_anti_affinity(self):
        print(">>> Trying to optimize slaves allocation for anti-affinity")

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

        evaluate_anti_affinity(self._nodes)


class CustomRoleDistribution(RoleDistribution):
    def __init__(self, nodes):
        super().__init__()
        self._nodes = nodes

    def create(self):
        self._set_replication()
        get_anti_affinity_score(self._nodes)
        return self._nodes

    def _set_replication(self):
        for n in self._nodes:
            if n.master_addr:
                master = first_true(self._nodes, pred=lambda m: m.addr == n.master_addr) 
                n.set_as_replica(master.node_id)


