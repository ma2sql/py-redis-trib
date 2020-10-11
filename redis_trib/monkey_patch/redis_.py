

def patch_redis_module():
    import redis

    def _parse_moving_slots(slots_exp, symbol):
        return dict(sl[1:-1].split(symbol)
                    for sl in slots_exp
                    if sl.find(symbol) != -1)
    
    def _parse_node_line(line):
        line_items = line.split(' ')
        node_id, addr, flags, master_id, ping, pong, epoch, \
            connected = line.split(' ')[:8]
        slots = [sl.split('-') for sl in line_items[8:] if sl[0] != '[']
        migrating = _parse_moving_slots(line_items[8:], '->-')
        importing = _parse_moving_slots(line_items[8:], '-<-')
        node_dict = {
            'node_id': node_id,
            'flags': flags.split(','),
            'master_id': master_id,
            'last_ping_sent': ping,
            'last_pong_rcvd': pong,
            'epoch': epoch,
            'slots': slots,
            'migrating': migrating,
            'importing': importing,
            'connected': True if connected == 'connected' else False
        }
        return addr, node_dict

    redis.client._parse_moving_slots = _parse_moving_slots
    redis.client._parse_node_line = _parse_node_line 

