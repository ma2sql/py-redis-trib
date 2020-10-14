import redis
import sys
import random
from collections import defaultdict
import time
import json
import functools
import math

verbose = True


MIGREATE_DEFAULT_TIMEOUT = 60000
MIGREATE_DEFAULT_PIPELINE = 10
CLUSTER_HASH_SLOTS = 16384
REBALANCE_DEFAULT_THRESHOLD = 2


class ClusterNode:
    def __init__(self, addr, password=None):
        ip, port = get_ip_port_by_addr(addr)

        self._r = None
        self._password = password
        self._info = {
            'host': ip,
            'port': port,
            'slots': {},
            'importing': {},
            'migrating': {}
        }
        self._dirty = False # True if we need to flush slots info into node.
        self._friends = []


    def __str__(self):
        return '{}:{}'.format(self._info['host'], self._info['port'])


    def connect(self, abort=False, force=False, decode_responses=False):
        if self._r and not force:
            return

        try:
            self._r = redis.StrictRedis(host=self._info['host'], 
                                  port=self._info['port'], 
                                  password=self._password,
                                  socket_connect_timeout=60,
                                  decode_responses=decode_responses)
            self._r.ping()
        except redis.RedisError as e:
            print('''[ERR] Sorry, can't connect to node {}'''.format(self))
            self._r = None
            if abort:
                sys.exit(1)


    def assert_cluster(self):
        info = self._r.info('cluster')
        if info.get('cluster_enabled') != 1:
            print('''[ERR] Node {} is not configured as a cluster node.'''.format(self))
            sys.exit(1)


    def assert_empty(self):
        cluster_info = self._r.cluster('info')
        info = self._r.info('keyspace')
        if (int(cluster_info['cluster_known_nodes']) != 1
            or info.get('db0')):
            print('[ERR] Node {} is not empty. '\
                  'Either the node already knows other nodes'\
                  ' (check with CLUSTER NODES) or '\
                  'contains some key in database 0.'.format(self))
            sys.exit(1)


    def _parse_slots(self, slots):
        migrating = {}
        importing = {}
        parsed_slots = []
        for s in slots:
            # ["[5461", "<", "b35b55daf26e84acdf17fed30d46ca97ccdd9169]"]
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
                parsed_slots.append(int(s[0]))

        return parsed_slots, migrating, importing


    def _slots_summary(self):
        summary = []
        for s in sorted(self._info['slots'].keys()):
            if not summary or (summary[-1][-1] + 1) != s:
                summary.append([])
            summary[-1][1:] = [s]
        return ','.join('-'.join(map(str, s)) for s in summary)


    def has_flag(self, flag):
        return flag in self._info['flags']


    def info_string(self):
        info_str = ''
        
        slots = self._slots_summary()
        role = 'M' if self.has_flag('master') else 'S'

        if self._info['replicate'] and self._dirty:
            info_str = 'S: {} {}'.format(self._info['name'], self)
        else:
            flags = ','.join(flag for flag in self._info['flags'] if flag != 'myself')
            info_str = '{role}: {node_id} {addr}\n'\
                 '    slots: {slots} ({slots_count} slots) {flags}'.format(
                    role=role, node_id=self._info['name'], addr=self, 
                    slots=slots, slots_count=len(self._info['slots']),
                    flags=flags)

        if self._info['replicate']:
            info_str += '\n    replicates {}'.format(self._info['replicate'])
        elif self.has_flag('master') and self._info.get('replicas'):
            info_str += '\n    {} additional replica(s)'.format(len(self._info['replicas']))

        return info_str   
    

    def get_config_signature(self):
        config = []
        for n in self._r.cluster('NODES').values():
            if not n['slots']:
                continue
            slots = ','.join('-'.join(map(str, s)) 
                            for s in n['slots']
                                if len(s) < 3)
            config.append('{}:{}'.format(n['node_id'], slots))
        return '|'.join(sorted(config))


    def load_info(self, **kwargs):
        self.connect()

        nodes = self._r.cluster('NODES')
        for addr, node in nodes.items():
            info = {
                'name': node['node_id'],
                'addr': addr.split('@')[0],
                'flags': node['flags'].split(','),
                'replicate': (node['master_id'] 
                              if node['master_id'] != '-' 
                              else False),
                'ping_sent': node['last_ping_sent'],
                'ping_recv': node['last_pong_rcvd'],
                'link_status': node['connected'],
            }

            if 'myself' in info['flags']:
                self._info.update(info)
                self._info['slots'] = {}
                self._dirty = False

                if node['slots']:
                    slots, migrating, importing = self._parse_slots(node['slots'])
                    self.add_slots(slots)
                    self._info['migrating'] = migrating
                    self._info['importing'] = importing
                
                cluster_info = self._r.cluster('INFO')
                for k, v in cluster_info.items():
                    self._info[k] = int(v) if k != 'cluster_state' else v

            elif kwargs.get('getfriends'):
                self._friends.append(info)


    def set_as_replica(self, node_id):
        self._info['replicate'] = node_id
        self._dirty = True


    @property
    def friends(self):
        return self._friends


    @property
    def r(self):
        return self._r


    @property
    def slots(self):
        return self._info['slots']

    
    @property
    def info(self):
        return self._info


    def add_slots(self, slots):
        for s in slots:
            self._info['slots'][s] = 'new'
        self._dirty = True


    def flush_node_config(self):
        if not self._dirty:
            return
        
        if self._info['replicate']:
            try:
                self._r.cluster('REPLICATE', self._info['replicate'])
            except:
                return
        else:
            new = []
            for s, val in self._info['slots'].items():
                if val == 'new':
                    new.append(s)
                    self._info['slots'][s] = True
            self._r.cluster('ADDSLOTS', *new)
        self._dirty = False    


class RedisTrib:
    def __init__(self):
        self._nodes = []
        self._fix = False
        self._errors = []
        self._timeout = MIGREATE_DEFAULT_TIMEOUT
        self._replicas = 0
        self._password = None
    

    def create_cluster(self, addrs, **kwargs):
        self._replicas = kwargs.get('replicas') or 0
        self._password = kwargs.get('password')

        print('>>> Creating cluster')
        for addr in addrs:
            node = ClusterNode(addr, password=self._password)
            node.connect(abort=True)
            node.assert_cluster()
            node.load_info()
            node.assert_empty()
            self._add_node(node)
        
        self._check_create_parameters()
        print('>>> Performing hash slots allocation on {} nodes...'.format(len(self._nodes)))
        self._alloc_slots()
        self._show_nodes()
        self.yes_or_die('Can I set the above configuration?')
        self._flush_nodes_config()
        print('>>> Nodes configuration updated')
        print('>>> Assign a different config epoch to each node')
        self._assign_config_epoch()
        print('>>> Sending CLUSTER MEET messages to join the cluster')
        self._join_cluster()
        # Give one second for the join to start, in order to avoid that
        # wait_cluster_join will find all the nodes agree about the config as
        # they are still empty with unassigned slots.
        time.sleep(1)
        self._wait_cluster_join()
        self._flush_nodes_config()
        # Reset the node information, so that when the
        # final summary is listed in check_cluster about the newly created cluster
        # all the nodes would get properly listed as slaves or masters
        self._reset_nodes()
        self._load_cluster_info_from_node(addrs[0])
        self._check_cluster()


    def _check_cluster(self, **kwargs):
        print('>>> Performing Cluster Check (using node {})'.format(self._nodes[0]))
        if not kwargs.get('quiet'):
            self._show_nodes()
        self._check_config_consistency()
        self._check_open_slots()
        self._check_slots_coverage()


    def _check_config_consistency(self):
        if not self._is_config_consistent():
            self._cluster_error('''[ERR] Nodes don't agree about configuration!''')
        else:
            print('[OK] All nodes agree about slots configuration.')


    def _check_open_slots(self):
        print('>>> Check for open slots...')
        open_slots = []
        for n in self._nodes:
            if len(n._info['migrating']) > 0:
                self._cluster_error(
                    '[WARNING] Node {} has slots in migrating state ({}).'.format(
                        n, ','.join(map(str, n._info['migrating'].keys()))
                    ))
                open_slots += n._info['migrating'].keys()
            if len(n._info['importing']) > 0:
                self._cluster_error(
                    '[WARNING] Node {} has slots in importing state ({}).'.format(
                        n, ','.join(map(str, n._info['importing'].keys()))
                    ))
                open_slots += n._info['importing'].keys()

        open_slots = list(set(open_slots))
        if len(open_slots) > 0:
            print('[WARNING] The following slots are open: {}'.format(
                ','.join(map(str, open_slots))))
        if self._fix:
            for slot in open_slots:
                self._fix_open_slot(slot)
            addr = '{}:{}'.format(
                self._nodes[0].info['host'],
                self._nodes[0].info['port']
            )
            self._load_cluster_info_from_node(addr)


    def _check_slots_coverage(self):
        print('>>> Check slots coverage...')
        slots = self._covered_slots()
        if len(slots) == CLUSTER_HASH_SLOTS:
            print('[OK] All {} slots covered.'.format(CLUSTER_HASH_SLOTS))
        else:
            self._cluster_error('[ERR] Not all {} slots are covered by nodes.'.format(
                CLUSTER_HASH_SLOTS))
            if self._fix:
                self._fix_slots_coverage()


    # Return the owner of the specified slot
    def _get_slot_owners(self, slot):
        owners = []
        for n in self._nodes:
            if n.has_flag('slave'):
                continue
            if n.slots.get(slot):
                owners.append(n)
            elif n.r.cluster('COUNTKEYSINSLOT', slot) > 0:
                owners.append(n)
        return owners


    # Move slots between source and target nodes using MIGRATE.
    #
    # Options:
    # :verbose -- Print a dot for every moved key.
    # :fix     -- We are moving in the context of a fix. Use REPLACE.
    # :cold    -- Move keys without opening slots / reconfiguring the nodes.
    # :update  -- Update nodes.info[:slots] for source/target nodes.
    # :quiet   -- Don't print info messages.
    def _move_slot(self, source, target, slot, **kwargs):
        o = {'pipeline': MIGREATE_DEFAULT_PIPELINE, **kwargs}

        # We start marking the slot as importing in the destination node,
        # and the slot as migrating in the target host. Note that the order of
        # the operations is important, as otherwise a client may be redirected
        # to the target node that does not yet know it is importing this slot.
        if not o.get('quiet'):
            print('Moving slot {} from {} to {}: '.format(slot, source, target))
        
        if not o.get('cold'):
            target.r.cluster('SETSLOT', slot, 'IMPORTING', source.info['name'])
            source.r.cluster('SETSLOT', slot, 'MIGRATING', target.info['name'])
        
        # Migrate all the keys from source to target using the MIGRATE command
        while True:
            migrate_keys = source.r.cluster('GETKEYSINSLOT', slot, o['pipeline'])
            if len(migrate_keys) == 0:
                break
            try:
                source.r.migrate(host=target.info['host'], 
                                 port=target.info['port'],
                                 keys=migrate_keys,
                                 destination_db=0,
                                 timeout=self._timeout,
                                 auth=self._password)
            except redis.RedisError as e:
                print(e.args)
                if o.get('fix') and 'BUSYKEY' in str(e):
                    print('*** Target key exists. Replacing it for FIX.')
                    source.r.migrate(host=target.info['host'], 
                                     port=target.info['port'],
                                     keys=migrate_keys,
                                     destination_db=0,
                                     timeout=self._timeout,
                                     auth=self._password,
                                     replace=True)
                else:
                    print('')
                    print('[ERR] Calling MIGRATE: {}'.format(e))
                    sys.exit(1)

            if o.get('dots'):
                print('.', end='', flush=True)

        # Set the new node as the owner of the slot in all the known nodes.
        if not o.get('quiet'):
            print('')
        
        if not o.get('cold'):
            for n in self._nodes:
                if n.has_flag('slave'):
                    continue
                n.r.cluster('SETSLOT', slot, 'NODE', target.info['name'])

        # Update the node logical config
        if o.get('update'):
            del source.info['slots'][slot]
            target.info['slots'][slot] = True

    
    # Return the node, among 'nodes' with the greatest number of keys
    # In the specified slot.
    def _get_node_with_most_keys_in_slot(self, nodes, slot):
        best = None
        best_numkeys = 0
        for n in nodes:
            if n.has_flag('slave'):
                continue
            numkeys = n.r.cluster('COUNTKEYSINSLOT', slot)
            if numkeys > best_numkeys or not best:
                best = n
                best_numkeys = numkeys
        return best, best_numkeys


    # Slot 'slot' was found to be in importing or migrating state in one or
    # more nodes. This function fixes this condition by migrating keys where
    # it seems more sensible.
    def _fix_open_slot(self, slot):
        print('>>> Fixing open slot {}'.format(slot))

        # Try to obtain the current slot owner, according to the current
        # nodes configuration.
        owners = self._get_slot_owners(slot)
        owner = None
        if len(owners) == 1:
            owner = owners[0]

        migrating = []
        importing = []

        for n in self._nodes:
            if n.has_flag('slave'):
                continue
            if n.info['migrating'].get(slot):
                migrating.append(n)
            elif n.info['importing'].get(slot):
                importing.append(n)
            elif n.r.cluster('COUNTKEYSINSLOT', slot) > 0 and n != owner:
                print('*** Found keys about slot {} in node {}!'.format(slot, n))
                importing.append(n)
        
        print('Set as migrating in: {}'.format(','.join(map(str, migrating))))
        print('Set as importing in: {}'.format(','.join(map(str, importing))))

        # If there is no slot owner, set as owner the slot with the biggest
        # number of keys, among the set of migrating / importing nodes.
        if not owner:
            # owner를 발견하지 못했다는 것은, 
            # 적어도 migrating 상태의 슬롯도 없었다는 것을 의미한다.
            # 또한, COUNTKEYSINSLOT으로 키를 가지고 있는 슬롯 또한 없었던 것이다.
            # 정말 슬롯을 잃었거나, 또는 IMPORTING 상태의 슬롯이 존재, 키는 없는 상태
            # 그리고, MIGRATING 상태의 슬롯이 정리된 상태?

            print('>>> Nobody claims ownership, selecting an owner...')
            # 클러스터 내에서 이 슬롯에 대한 키를 어느 곳에서도 가지고 있지 않다면, 
            # nodes의 첫 노드가 반환된다.
            # among the set of migrating / importing nodes?
            owner, numkeys = self._get_node_with_most_keys_in_slot(self._nodes, slot)

            # If we still don't have an owner, we can't fix it.
            # nodes가 비어있는게 아니라면, 이러한 경우는 있을 수가 없다..
            if not owner:
                print('''[ERR] Can't select a slot owner. Impossible to fix.''')
                sys.exit(1)
            
            # 그 어느 노드에서도 키를 가진 노드가 없었었다는 것을 의미한다.
            # IMPORTING은 하나 이상 존재하고 있을 수 있다.
            if numkeys == 0:
                owner = None
            else:
                # Use ADDSLOTS to assign the slot.
                print('*** Configuring {} as the slot owner'.format(owner))
                owner.r.cluster('SETSLOT', slot, 'STABLE')
                owner.r.cluster('ADDSLOTS', slot)

                # Make sure this information will propagate. Not strictly needed
                # since there is no past owner, so all the other nodes will accept
                # whatever epoch this node will claim the slot with.
                owner.r.cluster('BUMPEPOCH')

                # Remove the owner from the list of migrating/importing
                # nodes.
                migrating.remove(owner)
                importing.remove(owner)

        # If there are multiple owners of the slot, we need to fix it
        # so that a single node is the owner and all the other nodes
        # are in importing state. Later the fix can be handled by one
        # of the base cases above.
        #
        # Note that this case also covers multiple nodes having the slot
        # in migrating state, since migrating is a valid state only for
        # slot owners.
        if len(owners) > 1:
            owner = self._get_node_with_most_keys_in_slot(owners, slot)
            for n in owners:
                if n == owner:
                    continue
                n.r.cluster('DELSLOT', slot)
                n.r.cluster('SETSLOT', slot, 'IMPORTING', owner.info['name'])
                importing.remove(n) # Avoid duplicates
                importing.append(n)
            owner.r.cluster('BUMPEPOCH')

        # Case 1: The slot is in migrating state in one slot, and in
        #         importing state in 1 slot. That's trivial to address.
        if len(migrating) == 1 and len(importing) == 1:
            self._move_slot(migrating[0], importing[0], slot, dots=True, fix=True)
        
        # Case 2: There are multiple nodes that claim the slot as importing,
        # they probably got keys about the slot after a restart so opened
        # the slot. In this case we just move all the keys to the owner
        # according to the configuration.
        elif len(migrating) == 0 and len(importing) > 0 and owner:
            print('>>> Moving all the {} slot keys to its owner {}'.format(slot, owner))
            for node in importing:
                if node == owner:
                    continue
                self._move_slot(node, owner, slot, dots=True, fix=True, cold=True)
                print('>>> Setting {} as STABLE in {}'.format(slot, node))
                node.r.cluster('SETSLOT', slot, 'STABLE')

        # Case 3: There are no slots claiming to be in importing state, but
        # there is a migrating node that actually don't have any key. We
        # can just close the slot, probably a reshard interrupted in the middle.
        elif (len(importing) == 0 
                and len(migrating) == 1
                and len(migrating[0].r.cluster('GETKEYSINSLOT', slot, 10)) == 0):
            migrating[0].r.cluster('SETSLOT', slot, 'STABLE') 
        



        # Case 5: 빈 클러스터에 대해 리샤딩을 실행할 때, move_slot의 마지막 부분에서
        # CLUSTER SETSLOT NODE 명령을 전 노드로 실행할 때, IMPORTING 노드에 대해서
        # 실패가 발생한다면, 그 슬롯에 소유자는 없어져버리는 상태가 된다.
        # migrating은 해제될 것이다. 나머지 노드는 슬롯의 소유자를 IMPORTING 노드로 변경한다.
        # 이런 경우에는 단순히 IMPORTING 슬롯을 owner로 만들어주면 된다.
        # importing이 2개 이상이면, 첫 노드를 owner로 두고 나머지 노드에 대해서는 STABLE을 실행
        elif len(migrating) == 0 and len(importing) > 0 and not owner:
            owner = importing.pop(0)
            print('>>> Setting {} as the slot {} owner'.format(owner, slot))
            owner.r.cluster('SETSLOT', slot, 'NODE', owner.info['name'])
            for node in importing:
                print('>>> Setting {} as STABLE in {}'.format(slot, node))
                node.r.cluster('SETSLOT', slot, 'STABLE')

        else:
            print('''[ERR] Sorry, Redis-trib can't fix '''\
                  '''this slot yet (work in progress). '''\
                  '''Slot is set as migrating in {}, '''\
                  '''as importing in {}, owner is {}'''.format(
                      ','.join(migrating), ','.join(importing), owner))

    # Merge slots of every known node. If the resulting slots are equal
    # to ClusterHashSlots, then all slots are served.
    def _covered_slots(self):
        slots = {}
        for n in self._nodes:
            slots.update(n.slots)
        return slots


    def _nodes_with_keys_in_slot(self, slot):
        nodes = []
        for n in self._nodes:
            if n.has_flag('slave'):
                continue
            if len(n.r.cluster('GETKEYSINSLOT', slot, 1)) > 0:
                nodes.append(n)
        return nodes


    def _fix_slots_coverage(self):
        not_covered = list(range(0, CLUSTER_HASH_SLOTS)) - self._covered_slots().keys()
        print('>>> Fixing slots coverage...')
        print('List of not covered slots: {}'.format(','.join(map(str, not_covered))))

        # For every slot, take action depending on the actual condition:
        # 1) No node has keys for this slot.
        # 2) A single node has keys for this slot.
        # 3) Multiple nodes have keys for this slot.
        slots = {}
        for slot in not_covered:
            nodes = self._nodes_with_keys_in_slot(slot)
            slots[slot] = nodes
            print('Slot {} has keys in {} nodes: {}'.format(
                slot, len(nodes), ','.join(nodes)))

        none = {k: v for k, v in slots.items() if len(v) == 0}
        single = {k: v for k, v in slots.items() if len(v) == 1}
        multi = {k: v for k, v in slots.items() if len(v) > 1}

        # Handle case "1": keys in no node.
        if none:
            print('The folowing uncovered slots have no keys across the cluster:')
            print(','.join(map(str, none.keys())))
            self.yes_or_die('Fix these slots by covering with a random node?')
            # 기존 코드에서는 @nodes.sample을 호출
            # 여기서는 마스터만 골라내올 수 있는가?
            # slave에 대한 CLUSTER ADDSLOTS 가 실행되어버릴 수도 있음
            # 마스터 노드만 선별하고, 처리해야할 필요가 있음
            masters = [n for n in self._nodes if n.has_flag('master')]
            for slot, nodes in none.items():
                node = random.choice(masters)
                print('>>> Covering slot {} with {}'.format(slot, node))
                node.r.cluster('ADDSLOTS', slot)
            
            # Needed check
            # 반드시 필요한가? 
            # fix_slots_coverage가 발생하는 경우, 손실 노드의 CLUSTER FORGET을 전제로 함
            for m in masters:
                m.r.cluster('BUMPEPOCH')


        # Handle case "2": keys only in one node.
        if single:
            print('The folowing uncovered slots have keys in just one node:')
            print(','.join(map(str, single.keys())))
            self.yes_or_die('Fix these slots by covering with those nodes?')
            for slot, nodes in single.items():
                print('>>> Covering slot {} with {}'.format(slot, nodes[0]))
                nodes[0].r.cluster('ADDSLOTS', slot)
            
            # Needed check
            # 반드시 필요한가? 
            # fix_slots_coverage가 발생하는 경우, 손실 노드의 CLUSTER FORGET을 전제로 함
            for n in set(single.values()):
                n.r.cluster('BUMPEPOCH')
        
        # Handle case "3": keys in multiple nodes.
        if multi:
            print('The folowing uncovered slots have keys in multiple nodes:')
            print(','.join(map(str, multi.keys())))
            self.yes_or_die('Fix these slots by moving keys into a single node?')
            for slot, nodes in multi.items():
                target = self._get_node_with_most_keys_in_slot(nodes, slot)
                print('>>> Covering slot {} moving keys to {}'.format(slot, target))

                target.r.cluster('ADDSLOTS', slot)
                target.r.cluster('SETSLOT', slot, 'STABLE')

                for src in nodes:
                    if src == target:
                        continue
                    # Set the source node in 'importing' state (even if we will
                    # actually migrate keys away) in order to avoid receiving
                    # redirections for MIGRATE.
                    src.r.cluster('SETSLOT', slot, 'IMPORTING', target.info['name'])
                    self._move_slot(src, target, slot, dots=True, fix=True, cold=True)
                    src.r.cluster('SETSLOT', slot, 'STABLE')
            
            # Needed check
            # 반드시 필요한가? 
            # fix_slots_coverage가 발생하는 경우, 손실 노드의 CLUSTER FORGET을 전제로 함    
            for n in set(multi.values()):
                n.r.cluster('BUMPEPOCH')


    def _cluster_error(self, msg):
        self._errors.append(msg)
        print(msg)


    def _show_nodes(self):
        for n in self._nodes:
            print(n.info_string())


    def _load_cluster_info_from_node(self, node_addr, **kwargs):
        node = ClusterNode(node_addr, password=self._password)
        node.connect(abort=True, decode_responses=kwargs.get('decode_responses'))
        node.assert_cluster()
        node.load_info(getfriends=True)
        self._add_node(node)
        for f in node.friends:
            for flag in ['noaddr', 'disconnected', 'fail']:
                if flag in f['flags']:
                    continue
            fnode = ClusterNode(f['addr'], password=self._password)
            fnode.connect(decode_responses=kwargs.get('decode_responses'))
            if not fnode.r:
                continue

            try:
                fnode.load_info()
                self._add_node(fnode)
            except redis.RedisError as e:
                print('[ERR] Unable to load info for node {}'.format(fnode))

        self._populate_nodes_replicas_info()


    def _populate_nodes_replicas_info(self):
        for n in self._nodes:
            n._info['replicas'] = []
        
        for n in self._nodes:
            if n._info['replicate']:
                master = self._get_node_by_name(n._info['replicate'])
                if not master:
                    print('*** WARNING: {} claims to be slave of unknown node ID {}.'.format(
                        n, n._info['replicate']))
                else:
                    master._info['replicas'].append(n)


    def _get_node_by_name(self, node_id):
        node = None
        try:
            node = next(n for n in self._nodes 
                          if n.info['name'] == node_id.lower())
        except StopIteration:
            pass
        return node

    
    # Like get_node_by_name but the specified name can be just the first
    # part of the node ID as long as the prefix in unique across the
    # cluster.
    def _get_node_by_abbreviated_name(self, name):
        candidates = [n for n in self._nodes
                        if n.info['name'].startswith(name.lower())]
        return candidates[0] if candidates else None


    def _reset_nodes(self):
        self._nodes = []


    def _is_config_consistent(self):
        signatures=[]
        for n in self._nodes:
            signatures.append(n.get_config_signature())
        return len(set(signatures)) == 1


    def _wait_cluster_join(self):
        print('Waiting for the cluster to join')
        while not self._is_config_consistent():
            print('.', end='', flush=True)
            time.sleep(0.1)
        print('')


    def _join_cluster(self):
        first = self._nodes[0]
        for n in self._nodes[1:]:
            n.r.cluster('MEET', first._info['host'], first._info['port'])


    def _assign_config_epoch(self):
        config_epoch = 1
        for n in self._nodes:
            try:
                n.r.cluster('SET-CONFIG-EPOCH', config_epoch)
            except redis.RedisError:
                pass
            config_epoch += 1


    def _check_create_parameters(self):
        masters = len(self._nodes) / (self._replicas+1)
        if masters < 3:
            print('*** ERROR: Invalid configuration for cluster creation.')
            print('*** Redis Cluster requires at least 3 master nodes.')
            print('*** This is not possible with {}'\
                  ' nodes and {} replicas per node.'.format(
                            len(self._nodes), self._replicas))
            print('*** At least {} nodes are required.'.format(
                             3 * (self._replicas + 1)))
            sys.exit(1)


    def yes_or_die(self, msg):
        res = input('''{} (type 'yes|y|Y' to accept): '''.format(msg))
        if res.lower() not in ('yes', 'y'):
            print('*** Aborting...')
            sys.exit(1)
    

    def _flush_nodes_config(self):
        for n in self._nodes:
            n.flush_node_config()


    def _add_node(self, node):
        self._nodes.append(node)
        
    
    def _alloc_slots(self):
        nodes_count = len(self._nodes)
        masters_count = int(nodes_count / (self._replicas + 1))
        masters = []

        # The first step is to split instances by IP. This is useful as
        # we'll try to allocate master nodes in different physical machines
        # (as much as possible) and to allocate slaves of a given master in
        # different physical machines as well.
        #
        # This code assumes just that if the IP is different, than it is more
        # likely that the instance is running in a different physical host
        # or at least a different virtual machine.
        ips = defaultdict(list)
        for n in self._nodes:
            ips[n._info['host']].append(n)
        
        # Select master instances
        print('Using {} masters:'.format(masters_count))
        interleaved = []
        stop = False
        while not stop:
            # Take one node from each IP until we run out of nodes
            # across every IP.
            for ip, nodes in ips.items():
                if not nodes:
                    # if this IP has no remaining nodes, check for termination
                    if len(interleaved) == nodes_count:
                        # stop when 'interleaved' has accumulated all nodes
                        stop = True
                        continue
                else:
                    # else, move one node from this IP to 'interleaved'
                    interleaved.append(nodes.pop(0))

        masters = interleaved[:masters_count]
        interleaved = interleaved[masters_count:]
        nodes_count -= len(masters)

        for m in masters:
            print(m)

        # Rotating the list sometimes helps to get better intial
        # anti-affinity before the optimizer runs.
        if interleaved:
            interleaved.append(interleaved.pop(0))
        
        # Alloc slots on masters. After interleaving to get just the first N
        # should be optimal. With slaves is more complex, see later...
        slots_per_node = CLUSTER_HASH_SLOTS / masters_count
        first = 0
        cursor = 0.0
        for i, m in enumerate(masters):
            last = round(cursor + slots_per_node - 1)
            if last > CLUSTER_HASH_SLOTS or i == (len(masters)-1):
                last = CLUSTER_HASH_SLOTS - 1

            # Min step is 1.
            if last < first:
                last = first
            m.add_slots(range(first, last+1))
            first = last + 1
            cursor += slots_per_node

        # Select N replicas for every master.
        # We try to split the replicas among all the IPs with spare nodes
        # trying to avoid the host where the master is running, if possible.
        #
        # Note we loop two times. The first loop assigns the requsted
        # number of replicas to each master.  The second loop assigns any
        # remaining instances as extra replicas to masters.  Some masters
        # may end up with more than their requsted number of replicas, but
        # all nodes will be used.
        assignment_verbose = False

        for assign_extra_nodes in [False, True]:
            for m in masters:
                for assigned_replicas in range(self._replicas):
                    if nodes_count == 0:
                        break
                    
                    if assignment_verbose:
                        if not assign_extra_nodes:
                            print('Requesting total of {} replicas '\
                                  '({}) replicas assigned '\
                                  'so far with {} total remaining).'.format(
                                      self._replicas, assigned_replicas, nodes_count))
                        else:
                            print('Assigning extra instance to replication '\
                                  'role too ({} remaining)'.format(nodes_count))
                    
                    # Return the first node not matching our current master
                    node = next([i, n] for i, n in enumerate(interleaved) 
                                if n.info['host'] != m.info['host'])

                    # If we found a node, use it as a best-first match.
                    # Otherwise, we didn't find a node on a different IP, so we
                    # go ahead and use a same-IP replica.
                    if node:
                        slave = node
                        interleaved.remove(idx)
                    else:
                        slave = interleaved.pop(0)

                    slave.set_as_replica(m.info['name'])
                    nodes_count -= 1
                    print('Adding replica {} to {}'.format(slave, m))

                    # If we are in the "assign extra nodes" loop,
                    # we want to assign one extra replica to each
                    # master before repeating masters.
                    # This break lets us assign extra replicas to masters
                    # in a round-robin way.
                    if assign_extra_nodes:
                        break

        self._optimize_anti_affinity()


    def _optimize_anti_affinity(self):
        score, aux = self._get_anti_affinity_score()
        if score == 0:
            return
        
        print('>>> Trying to optimize slaves allocation for anti-affinity')

        maxiter = 500 * len(self._nodes) # Effort is proportional to cluster size...
        for _ in range(maxiter):
            score, offenders = self._get_anti_affinity_score()
            # Optimal anti affinity reached
            if score == 0: 
                break

            # We'll try to randomly swap a slave's assigned master causing
            # an affinity problem with another random slave, to see if we
            # can improve the affinity.
            first = random.choice(offenders)
            nodes = filter(lambda x: n != first and n.info['replicate'], self._nodes)
            if nodes:
                break
            second = random.choice(nodes) 

            first_master_id = first.info['replicate']
            second_master_id = second.info['replicate']

            first.set_as_replica(second_master_id)
            second.set_as_replica(first_master_id)

            new_score, aux = self._get_anti_affinity_score()
            # If the change actually makes thing worse, revert. Otherwise
            # leave as it is becuase the best solution may need a few
            # combined swaps.
            if new_score > score:
                first.set_as_replica(first_master)
                second.set_as_replica(second_master)

            maxiter -= 1
        
        score,aux = self._get_anti_affinity_score()
        if score == 0:
            print('[OK] Perfect anti-affinity obtained!')
        elif score >= 10000:
            print('[WARNING] Some slaves are in the same host as their master')
        else:
            print('[WARNING] Some slaves of the same master are in the same host')


    # Return the anti-affinity score, which is a measure of the amount of
    # violations of anti-affinity in the current cluster layout, that is, how
    # badly the masters and slaves are distributed in the different IP
    # addresses so that slaves of the same master are not in the master
    # host and are also in different hosts.
    #
    # The score is calculated as follows:
    #
    # SAME_AS_MASTER = 10000 * each slave in the same IP of its master.
    # SAME_AS_SLAVE  = 1 * each slave having the same IP as another slave
    #                      of the same master.
    # FINAL_SCORE = SAME_AS_MASTER + SAME_AS_SLAVE
    #
    # So a greater score means a worse anti-affinity level, while zero
    # means perfect anti-affinity.
    #
    # The anti affinity optimizator will try to get a score as low as
    # possible. Since we do not want to sacrifice the fact that slaves should
    # not be in the same host as the master, we assign 10000 times the score
    # to this violation, so that we'll optimize for the second factor only
    # if it does not impact the first one.
    #
    # The function returns two things: the above score, and the list of
    # offending slaves, so that the optimizer can try changing the
    # configuration of the slaves violating the anti-affinity goals. 
    def _get_anti_affinity_score(self):
        score = 0
        offending = [] # List of offending slaves to return to the caller

        # First, split nodes by host
        host_to_node = defaultdict(list)
        for n in self._nodes:
            host = n._info['host']
            host_to_node[host].append(n)
        
        # Then, for each set of nodes in the same host, split by
        # related nodes (masters and slaves which are involved in
        # replication of each other)
        for host, nodes in host_to_node.items():
            related = defaultdict(list)
            for n in nodes:
                if not n._info['replicate']:
                    name = n._info['name']
                    related[name].append('master')
                else:
                    name = n._info['replicate']
                    related[name].append('slave')
        
            # Now it's trival to check, for each related group having the
            # same host, what is their local score.
            for node_id, types in related.items():
                if len(types) < 2: 
                    continue

                sorted_type = sorted(types)
                if types[0] == 'master':
                    score += 10000 * (len(types)-1)
                else:
                    score += (1 * len(types))
                
                # Populate the list of offending nodes
                for n in self._nodes:
                    if (n.info['replicate'] == node_id
                        and n.info['host'] == host):
                        offending.append(n)

        return score, offending


    def _show_cluster_info(self):
        masters = 0
        keys = 0
        for n in self._nodes:
            if n.has_flag('master'):
                n_keys = n.r.dbsize()
                print('{} ({}...) -> {} keys | {} slots | {} slaves.'.format(
                    n, n.info['name'][:8], n_keys, len(n.slots.keys()), len(n.info['replicas'])))
                masters += 1
                keys += n_keys

        print('[OK] {} keys in {} masters.'.format(keys, masters))
        keys_per_slot = keys / float(CLUSTER_HASH_SLOTS)
        print('{:.2f} keys per slot on average.'.format(keys_per_slot))


    def check_cluster(self, addr, **kwargs):
        if kwargs.get('password'):
            self._password = kwargs['password']

        self._load_cluster_info_from_node(addr)
        self._check_cluster()


    def info_cluster(self, addr, **kwargs):
        if kwargs.get('password'):
            self._password = kwargs['password']

        self._load_cluster_info_from_node(addr)
        self._show_cluster_info()


    # This function returns the master that has the least number of replicas
    # in the cluster. If there are multiple masters with the same smaller
    # number of replicas, one at random is returned.    
    def _get_master_with_least_replicas(self):
        masters = [n for n in self._nodes if n.has_flag('master')]
        sorted_masters = sorted(masters, key=lambda n: len(n.info['replicas']))
        return sorted_masters[0] if sorted_masters else None


    def addnode_cluster(self, existring_addr, new_addr, **kwargs):
        if kwargs.get('password'):
            self._password = kwargs['password']

        print('>>> Adding node {} to cluster {}'.format(new_addr, existring_addr))

        # Check the existing cluster
        self._load_cluster_info_from_node(existring_addr)
        self._check_cluster()

        # If --master-id was specified, try to resolve it now so that we
        # abort before starting with the node configuration.
        if kwargs.get('slave'):
            if kwargs.get('master_id'):
                master = self._get_node_by_name(kwargs['master_id'])
                if not master:
                    print('[ERR] No such master ID {}'.format(kwargs['master_id']))
            else:
                master = self._get_master_with_least_replicas()
                print('Automatically selected master {}'.format(master.info['name']))

        # Add the new node
        new_node = ClusterNode(new_addr, password=self._password)
        new_node.connect(abort=True)
        new_node.assert_cluster()
        new_node.load_info()
        new_node.assert_empty()
        first = self._nodes[0].info
        self._add_node(new_node)

        # Send CLUSTER MEET command to the new node
        print('>>> Send CLUSTER MEET to node {}'\
              ' to make it join the cluster.'.format(new_node))
        new_node.r.cluster('MEET', first['host'], first['port'])

        # Additional configuration is needed if the node is added as
        # a slave.
        if kwargs.get('slave'):
            self._wait_cluster_join()
            print('>>> Configure node as replica of {}.'.format(master))
            new_node.r.cluster('REPLICATE', master.info['name'])

        print('[OK] New node added correctly.')

    
    def delnode_cluster(self, work_addr, node_id, **kwargs):
        if kwargs.get('password'):
            self._password = kwargs['password']
        
        print('>>> Removing node {} from cluster {}'.format(node_id, work_addr))

        # Load cluster information
        self._load_cluster_info_from_node(work_addr)

        # Check if the node exists and is not empty
        node = self._get_node_by_name(node_id)

        if not node:
            print('[ERR] No such node ID {}'.format(node_id))
            sys.exit(1)
        
        if len(node.slots.keys()) != 0:
            print('[ERR] Node {} is not empty! '\
                  'Reshard data away and try again.'.format(node))
            sys.exit(1)
        
        # Send CLUSTER FORGET to all the nodes but the node to remove
        print('>>> Sending CLUSTER FORGET messages to the cluster...')
        for n in self._nodes:
            if n == node:
                continue
            if n.info['replicate'] and n.info['replicate'].lower() == node_id:
                # Reconfigure the slave to replicate with some other node
                master = self._get_master_with_least_replicas()
                print('>>> {} as replica of {}'.format(n, master))
                n.r.cluster('REPLICATE', master.info['name'])
            
            n.r.cluster('forget', node_id)
        
        # Finally shutdown the node
        print('>>> SHUTDOWN the node.')
        if kwargs.get('shutdown_command'):
            print('shutdown_command: {}'.format(kwargs['shutdown_command']))
            try:
                node.r.execute_command(kwargs['shutdown_command'])
            except redis.exceptions.ConnectionError as e:
                if not str(e).startswith('Connection closed by server.'):
                    raise
        else:
            node.r.shutdown()


    def fix_cluster(self, addr, **kwargs):
        self._fix = True
        if kwargs.get('timeout'):
            self._timeout = kwargs['timeout']
        
        if kwargs.get('password'):
            self._password = kwargs['password']

        self._load_cluster_info_from_node(addr)
        self._check_cluster()


    def reshard_cluster(self, addr):
        opt = {'pipeline': MIGREATE_DEFAULT_PIPELINE, **kwargs}

        if kwargs.get('password'):
            self._password = kwargs['password']

        if kwargs.get('timeout'):
            self._timeout = kwargs['timeout']

        self._load_cluster_info_from_node(addr)
        self._check_cluster()

        if len(self._errors) != 0:
            print('*** Please fix your cluster problems before resharding')
            sys.exit(1)
        
        # Get number of slots
        if opt.get('slots'):
            numslots = int(opt['slots'])
        else:
            numslots = 0
            while numslots <= 0 or numslots > CLUSTER_HASH_SLOTS:
                msg = 'How many slots do you want to move'\
                      ' (from 1 to {})? '.format(CLUSTER_HASH_SLOTS)
                numslots = int(input(msg))

        # Get the target instance
        if opt.get('to'):
            target = self._get_node_by_name(opt['to'])
            if not target or target.has_flag('slave'):
                print('*** The specified node is not known'\
                      ' or not a master, please retry.')
                sys.exit(1)
        else:
            target = None
            while not target:
                node_id = input('What is the receiving node ID? ')
                target = self._get_node_by_name(node_id)
                if not target or target.has_flag('slave'):
                    print('*** The specified node is not known'\
                          ' or not a master, please retry.')
                    target = None
        
        # Get the source instances
        sources = []
        if opt.get('from'):
            for node_id in opt['from'].split(','):
                if node_id == 'all':
                    sources = 'all'
                    break
                src = self._get_node_by_name(node_id)
                if not src or src.has_flag('slave'):
                    print('*** The specified node is not known '\
                          'or is not a master, please retry.')
                    sys.exit(1)
                sources.append(src)
        else:
            print('''Please enter all the source node IDs.''')
            print('''  Type 'all' to use all the nodes as source nodes for the hash slots.''')
            print('''  Type 'done' once you entered all the source nodes IDs.''')
            while True:
                line = input('Source node #{}:'.format(len(sources) + 1))
                src = self._get_node_by_name(line)
                if line == 'done':
                    break
                elif line == 'all':
                    sources = 'all'
                    break
                elif not src or src.has_flag('slave'):
                    print('*** The specified node is not known '\
                          'or is not a master, please retry.')
                elif src.info['name'] == target.info['name']:
                    print('*** It is not possible to use the target node as source node.')
                else:
                    sources.append(src)
        
        if len(sources) == 0:
            print('*** No source nodes given, operation aborted')
            sys.exit(1)
        
        # Handle soures == all.
        if sources == 'all':
            sources = []
            for n in self._nodes:
                if n.info['name'] == target.info['name']:
                    continue
                if n.has_flag('slave'):
                    continue
                sources.append(n)
        
        # Check if the destination node is the same of any source nodes.
        if sources.index(target):
            print('*** Target node is also listed among the source nodes!')
            sys.exit(1)
        
        print('\nReady to move {} slots.'.format(numslots))
        print('  Source nodes:')
        for s in sources:
            print('    {}'.format(s.info_string))
        print('  Destination node:')
        print('    {}'.format(target.info_string))
        reshard_table = self._compute_reshard_table(sources, numslots)
        print('  Resharding plan:')
        self._show_reshard_table(reshard_table)
        return
        if not opt.get('yes'):
            yesno = input('Do you want to proceed with the proposed reshard plan (yes/no)? ')
            if yesno != 'yes':
                sys.exit(1)
            for e in reshard_table:
                self._move_slot(e['source'], 
                                target, 
                                e['slot'], 
                                dots=True, 
                                pipeline=opt.get('pipeline'))


    # Given a list of source nodes return a "resharding plan"
    # with what slots to move in order to move "numslots" slots to another
    # instance.
    def _compute_reshard_table(self, sources, numslots):
        moved = []
        # Sort from bigger to smaller instance, for two reasons:
        # 1) If we take less slots than instances it is better to start
        #    getting from the biggest instances.
        # 2) We take one slot more from the first instance in the case of not
        #    perfect divisibility. Like we have 3 nodes and need to get 10
        #    slots, we take 4 from the first, and 3 from the rest. So the
        #    biggest is always the first.    
        sorted_sources = sorted(sources, key=lambda s: len(s.slots.keys()))
        source_tot_slots = sum(len(s.slots.keys()) for s in sources)
        for i, s in enumerate(sorted_sources):
            # Every node will provide a number of slots proportional to the
            # slots it has assigned.
            n = float(numslots) / source_tot_slots * len(s.slots.keys())
            if i == 0:
                n = math.ceil(n)
            else:
                n = math.floor(n)
            for slot in sorted(s.slots.keys())[0:n]:
                if len(moved) < numslots:
                    moved.append({'source': s, 'slot': slot})

        return moved
    

    def _show_reshard_table(self, table):
        for e in table:
            print('    Moving slot {} from {}'.format(
                  e.get('slot'), e['source'].info['name']))
    

    def rebalance_cluster(self, addr, **kwargs):
        opt = {'pipeline': MIGREATE_DEFAULT_PIPELINE,
             'threshold': REBALANCE_DEFAULT_THRESHOLD,
             **kwargs}

        if kwargs.get('password'):
            self._password = kwargs['password']

        # Load nodes info before parsing options, otherwise we can't
        # handle --weight.
        self._load_cluster_info_from_node(addr)

        threshold = int(opt.get('threshold'))
        # Unused
        autoweights = opt.get('auto-weights')
        weights = {}
        for w in opt.get('weight') or []:
            field, weight = w.split('=')
            node = self._get_node_by_abbreviated_name(field)
            if not node or not node.has_flag('master'):
                print('*** No such master node {}'.format(field))
                sys.exit(1)
            weights[node.info['name']] = float(weight)
        useempty = opt.get('use_empty_masters')

        # Assign a weight to each node, and compute the total cluster weight.
        total_weight = 0
        nodes_involved = 0
        for n in self._nodes:
            if n.has_flag('master'):
                if not useempty and len(n.slots) == 0:
                    continue
                n.info['w'] = weights.get(n.info['name']) or 1
                total_weight += n.info['w']
                nodes_involved += 1
                
        # Check cluster, only proceed if it looks sane.
        self._check_cluster(quiet=True)
        if len(self._errors) != 0:
            print('*** Please fix your cluster problems before rebalancing')
            sys.exit(1)
        
        # Calculate the slots balance for each node. It's the number of
        # slots the node should lose (if positive) or gain (if negative)
        # in order to be balanced.
        threshold = float(opt['threshold'])
        threshold_reached = False
        for n in self._nodes:
            if n.has_flag('master'):
                if not n.info.get('w'):
                    continue
                expected = int((float(CLUSTER_HASH_SLOTS) / total_weight)
                            * int(n.info['w']))
                n.info['balance'] = len(n.slots) - expected
                # Compute the percentage of difference between the
                # expected number of slots and the real one, to see
                # if it's over the threshold specified by the user.
                over_threshold = False
                if threshold > 0:
                    if len(n.slots) > 0:
                        err_perc = abs(100 - (100.0 * expected / len(n.slots)))
                        if err_perc > threshold:
                            over_threshold = True
                    elif expected > 0:
                        over_threshold = True
                if over_threshold:
                    threshold_reached = True


        for n in self._nodes:
            print(n, n.info.get('w'), n.info.get('balance'))

        if not threshold_reached:
            print('*** No rebalancing needed! '\
                  'All nodes are within the {}% threshold.'.format(threshold))
            return

        # Only consider nodes we want to change
        sn = [n for n in self._nodes 
                if n.has_flag('master') and n.info['w']]

        # Because of rounding, it is possible that the balance of all nodes
        # summed does not give 0. Make sure that nodes that have to provide
        # slots are always matched by nodes receiving slots. 
        total_balance = sum(n.info['balance'] for n in sn)
        while total_balance > 0:
            for n in sn:
                if n.info['balance'] < 0 and total_balance > 0:
                    n.info['balance'] -= 1
                    total_balance -= 1
        
        # Sort nodes by their slots balance.
        sn = sorted(sn, key=lambda n: n.info['balance'])

        print('>>> Rebalancing across {} nodes.'\
              ' Total weight = {}'.format(nodes_involved, total_weight))

        if verbose:
            for n in sn:
                print('{} balance is {} slots'.format(n, n.info['balance']))
        
        # Now we have at the start of the 'sn' array nodes that should get
        # slots, at the end nodes that must give slots.
        # We take two indexes, one at the start, and one at the end,
        # incrementing or decrementing the indexes accordingly til we
        # find nodes that need to get/provide slots.
        dst_idx = 0
        src_idx = len(sn) - 1
        
        while dst_idx < src_idx:
            dst = sn[dst_idx]
            src = sn[src_idx]
            numslots = min(map(abs, [dst.info['balance'], src.info['balance']]))

            if numslots > 0:
                print('Moving {} slots from {} to {}'.format(numslots, src, dst))

                # Actaully move the slots.
                reshard_table = self._compute_reshard_table([src], numslots)
                if len(reshard_table) != numslots:
                    print('*** Assertio failed: Reshard table != number of slots')
                    sys.exit(1)
                if opt.get('simulate'):
                    print('#' * len(reshard_table))
                else:
                    for e in reshard_table:
                        self._move_slot(e['source'], dst, e['slot'],
                            quiet=True, dots=False, 
                            update=True, pipeline=opt['pipeline'])
                        print('#', end='', flush=True)
                print()

            # Update nodes balance.
            dst.info['balance'] += numslots
            src.info['balance'] -= numslots
            if dst.info['balance'] == 0:
                dst_idx += 1
            if src.info['balance'] == 0:
                src_idx -= 1
    

    def call_cluster(self, addr, cmd, **kwargs):
        if kwargs.get('password'):
            self._password = kwargs['password']

        cmd = [cmd[0].upper()] + cmd[1:]
        # Load cluster information
        self._load_cluster_info_from_node(addr, decode_responses=True)
        print('>>> Calling {}'.format(' '.join(cmd)))
        results = {}
        for n in self._nodes:
            try:
                #n.r.set_response_callback('INFO', bytes)
                res = n.r.execute_command(*cmd)
                results[str(n)] = res
            except redis.RedisError as e:
                print('{}: {}'.format(n, res))
        print(json.dumps(results, indent=4))
        for k, v in results.items():
            print(k, v)


    def import_cluster(self, addr, **kwargs):
        if kwargs.get('password'):
            self._password = kwargs['password']        

        source_addr = kwargs.get('from')
        print('>>> Importing data from {} to cluster {}'.format(
            source_addr, addr))

        use_copy = kwargs.get('copy') or False
        use_replace = kwargs.get('replace') or False

        # Check the existing cluster.
        self._load_cluster_info_from_node(addr)
        self._check_cluster()

        # Connect to the source node.
        print('>>> Connecting to the source Redis instance')
        src_host, src_port = source_addr.split(':')
        source = redis.StrictRedis(host=host, port=src_port, 
                                   password=kwargs.get('from-password'))
        if int(source.info['cluster_enabled']) == 1:
            print('[ERR] The source node should not be a cluster node.')
        print('*** Importing {} keys from DB 0'.format(source.dbsize()))

        # Build a slot -> node map
        slots = {s: n for n in self._nodes
                          for s in n.slots.keys()}
        
        # Use SCAN to iterate over the keys, migrating to the
        # right node as needed.
        for k in source.scan_iter(count=1000):
            # Migrate keys using the MIGRATE command.
            slot = key_to_slot(k)
            target = slots[slot]
            print('Migrating {} to {}'.format(k, target), end='')
            try:
                source.migrate(target.info['host'], 
                               target.info['port'], 
                               k, # key
                               0, # db
                               self._timeout, # timeout
                               copy=use_copy, # copy
                               replace=use_replace, # replace 
                               auth=self._password # password
                               )
            except redis.RedisError as e:
                print(e)
            else:
                print('OK')

if __name__ == '__main__':
    '''
    RedisTrib().create_cluster(
        ['192.168.56.101:7001', 
         '192.168.56.101:7002', 
         '192.168.56.101:7003']
    )

    RedisTrib().check_cluster('192.168.56.101:7001')
    RedisTrib().info_cluster('192.168.56.101:7001')
    '''
    '''
    RedisTrib().check_cluster('192.168.56.101:7001')
    RedisTrib().fix_cluster('192.168.56.101:7001')
    '''
    RedisTrib().check_cluster('192.168.56.101:7001')
    RedisTrib().fix_cluster('192.168.56.101:7001')

## [DONE]
#   - create_cluster_cmd
#   - check_cluster_cmd
#   - info_cluster_cmd
#   - addnode_cluster_cmd
#   - delnode_cluster_cmd
#   - fix_cluster_cmd
#   - rebalance_cluster_cmd
#   - reshard_cluster_cmd
#   - call_cluster_cmd

## [TODO]
#   - import_cluster_cmd
#   - help_cluster_cmd
#   - set_timeout_cluster_cmd

