import redis
import types

CLUSTER_HASH_SLOTS = 16384


# TODO: Create a print function that support VERBOSE mode
def print_verbose(*args, **kwargs):
    print(*args, **kwargs)


class ClusterNodeException(Exception): pass
class CannotConnectToRedis(ClusterNodeException): pass


class ClusterNode:
    def __init__(self, addr, password=None):
        s = addr.split('@')[0].split(':')
        if len(s) < 2:
            print(f"Invalid IP or Port (given as {addr}) - use IP:Port format")
            sys.exit(1)
        
        self._host = ':'.join(s[:-1])
        self._port = s[-1]
        self._password = password
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
    def slots(self):
        return self._slots

    def connect(self, abort=False):
        if self._r:
            return
        print_verbose(f"Connecting to node {self}: ", end="")
        try:
            self._r = redis.StrictRedis(self.host, self.port,
                                        password=self._password,
                                        timeout=60)
            self._r.ping()
        except redis.exceptions.ConnectionError:
            print_verbose(f"[ERR] Sorry, can't connect to node '{self}'")
            if abort:
                raise CannotConnectToRedis(f"[ERR] Sorry, can't connect to node '{self}'")
            self._r = None
        print_verbose("OK")

    def assert_cluster(self):
        cluster_enabled = self._r.info().get('cluster_enabled') or 0
        if int(cluster_enabled) != 1:
            raise AssertClusterError(f"[ERR] Node {self} is not configured as a cluster node.")

    def assert_empty(self):
        cluster_known_nodes = self._r.cluster('INFO').get('cluster_known_nodes') or 0
        keyspace = self._r.info.get('db0')
        if (not keyspace or cluster_known_nodes != 1):
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
        if not self._cluster_nodes:
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

    def set_master(self, master):
        self._master = master

    def flush_node_config(self):
        if not self._dirty: return

        if self._replicate:
            try:
                self._r.cluster("REPLICATE", self._replicate)
            except redis.exceptions.ResponseError:
                pass
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
   
    '''
            if self.info[:replicate] and @dirty
            is = "S: #{self.info[:name]} #{self.to_s}"
        else
            is = "#{role}: #{self.info[:name]} #{self.to_s}\n"+
            "   slots:#{slots} (#{self.slots.length} slots) "+
            "#{(self.info[:flags]-["myself"]).join(",")}"
        end
        if self.info[:replicate]
            is += "\n   replicates #{info[:replicate]}"
        elsif self.has_flag?("master") && self.info[:replicas]
            is += "\n   #{info[:replicas].length} additional replica(s)"
        end
        is 
    ''' 
    def info_string(self):
        role = "M" if "master" in self._flags else "S" 
        info_str = ""
        if self._replicate and self._dirty:
            info_str = f"S: {self._replicate} {self}"
        else:
            info_str = f"{role}: {self._node_id} {self}\n"\
                       f"   slots:{self._summarize_slots(self._slots)} ({len(self._slots)}) "\
                       f"{','.join(filter(lambda flag: flag != 'myself', self._flags))}"
        if self._replicate:
            info_str += "\n    replicates {self._replicate}"
        elif "master" in self._flags and self._replicas:
            info_str += "\n    {len(self._replicas)} additional replica(s)"
        
        return info_str

    def assign_config_epoch(self, config_epoch):
        pass

    def cluster_meet(self, host, port):
        pass

    def get_config_signature(self):
        pass

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
                slave.set_master(master)
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
