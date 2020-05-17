import redis
from .util import xprint, group_by
from .exceptions import AssertEmptyError
from more_itertools import first_true
from functools import reduce
import collections
from .const import CLUSTER_HASH_SLOTS
import time

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
        self._dbsize = None

    def __eq__(self, obj):
        return self is obj

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

    @property
    def replicas(self):
        return self._replicas

    @property
    def flags(self):
        return self._flags

    @property
    def r(self):
        return self._r

    @property
    def migrating(self):
        return self._migrating

    @property
    def importing(self):
        return self._importing

    def is_slave(self):
        return self.replicate is not None

    def is_master(self):
        return not self.is_slave()

    def add_replica(self, node):
        self.replicas.append(node)

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
        self._dbsize = keyspace and keyspace.get('keys')

    def load_info(self):
        for k, n in self._get_cluster_nodes().items():
            flags = self._parse_flags(n.get('flags'))
            if 'myself' in flags:
                host, port = k.split('@')[0].split(':')[:2]
                if host:
                    self._host, self._port = host, port
                self._node_id = n['node_id']
                self._flags = flags
                if n['master_id'] != '-':
                    self._replicate = n['master_id']
                (slots, migrating, importing) = self._parse_slots(n['slots'])
                self.add_slots(slots)
                self._migrating = migrating
                self._importing = importing
                self._dirty = False
                break

    def _parse_flags(self, flags):
        return flags.split(',')

    def _get_cluster_nodes(self):
        if not self._cluster_nodes:
            self._refresh_cluster_nodes()
        return self._cluster_nodes

    def _refresh_cluster_nodes(self):
        self._cluster_nodes = self._r.cluster('NODES')

    @property
    def dbsize(self):
        if not self._dbsize:
            self._dbsize = self._r.dbsize()
        return self._dbsize

    @property
    def friends(self):
        for addr, n in  self._get_cluster_nodes().items():
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
                       f"   slots:{self._summarize_slots(self._slots)} ({len(self._slots)} slots) "\
                       f"{','.join(filter(lambda flag: flag != 'myself', self._flags))}"
        if self._replicate:
            info_str += f"\n   replicates {self._replicate}"
        elif "master" in self._flags and self._replicas:
            info_str += f"\n   {len(self._replicas)} additional replica(s)"
        
        return info_str

    def set_config_epoch(self, config_epoch):
        self._r.cluster('SET-CONFIG-EPOCH', config_epoch) 

    def cluster_meet(self, host, port):
        self._r.cluster('MEET', host, port)

    def get_config_signature(self):
        # Return a single string representing nodes and associated slots.
        # TODO: remove slaves from config when slaves will be handled
        # by Redis Cluster.
        self._refresh_cluster_nodes()
        config = []
        for n in self._get_cluster_nodes().values():
            slots, *_ = self._parse_slots(n['slots'])
            if 'master' not in self._parse_flags(n['flags']):
                continue
            config.append(f"{n['node_id']}:{self._summarize_slots(slots)}")
        return '|'.join(sorted(config))

    def cluster_replicate(self, master):
        self._cluster_replicate(master.node_id)

    def _cluster_replicate(self, node_id):
        self._r.cluster('REPLICATE', node_id)


class ClusterNodes:
    def __init__(self, nodes=None):
        self._nodes = nodes or []

    def __iter__(self):
        for n in self._nodes:
            yield n

    def __len__(self):
        return len(self._nodes)

    @property
    def masters(self):
        return [n for n in self if n.is_master()]

    def add_node(self, node):
        self._nodes.append(node)
 
    def get_master(self, node):
        return first_true(self, pred=lambda m: m.node_id == node.replicate)

    def is_config_consistent(self):
        signatures=[]
        for n in self:
            signatures.append(n.get_config_signature())
        return len(set(signatures)) == 1
    
    def wait_cluster_join(self):
        print("Waiting for the cluster to join")
        while not self.is_config_consistent():
            print(".", end="", flush=True)
            time.sleep(1)
        print()

    def opened_slots(self):
        for n in self: 
            yield n, n.migrating, n.importing
           
    def covered_slots(self):
        return reduce(lambda a, b: {**a, **b.slots}, self, {})

    def show_nodes(self):
        for n in self:
            print(n.info_string())

    def populate_nodes_replicas_info(self):
        for n in self:
            if n.is_slave():
                master = self.get_master(n)
                if master:
                    master.add_replica(n)

    def alloc_slots(self):
        slots_per_node = float(CLUSTER_HASH_SLOTS) / len(self.masters)
        first = 0
        cursor = 0.0
        for i, m in enumerate(self.masters):
            last = round(cursor + slots_per_node - 1)
            if last > CLUSTER_HASH_SLOTS or i == len(self.masters) - 1:
                last = CLUSTER_HASH_SLOTS - 1
            if last < first:
                last = first
            m.add_slots(list(range(first, last+1)))
            first = last+1
            cursor += slots_per_node

    def flush_nodes_config(self):
        for n in self:
            n.flush_node_config()

    def assign_config_epoch(self):
        for config_epoch, m in enumerate(self.masters, 1):
            xprint(f"[WARNING] {m}: {config_epoch}")
            m.set_config_epoch(config_epoch)

    def join_all_cluster(self):
        first = self[0]
        for n in self[1:]:
            n.cluster_meet(first.host, first.port)

    def join_cluster(self, new_node):
        first = self[0]
        new_node.cluster_meet(first.host, first.port)

    def get_anti_affinity_score(self):
        score = 0
        # List of offending slaves to return to the caller
        offending = []
        # First, split nodes by host
        host_to_node = group_by(self, key=lambda n: n.host)
    
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
    
    def evaluate_anti_affinity(self):
        score, *_ = self.get_anti_affinity_score()
        if score == 0:
            xprint("[OK] Perfect anti-affinity obtained!")
        elif score >= 10000:
            xprint("[WARNING] Some slaves are in the same host as their master")
        else:
            xprint("[WARNING] Some slaves of the same master are in the same host")
    
    def is_config_consistent(self):
        signatures=[]
        for n in self:
            signatures.append(n.get_config_signature())
        return len(set(signatures)) == 1
    
    def wait_cluster_join(self):
        print("Waiting for the cluster to join")
        while not self.is_config_consistent():
            print(".", end="", flush=True)
            time.sleep(1)
        print()

    def get_node_by_name(self, node_id):
        return first_true(self, pred=lambda m: m.node_id == node_id)


    def get_master_with_least_replicas(self):
        return sorted(self.masters, key=lambda n: len(n.replicas))[0]

