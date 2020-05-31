

def patch_click_module():
    import click
    def set_verbose(*args, **kwargs):
        from redis_trib.xprint import xprint, LOG_LEVEL_VERBOSE
        xprint.set_loglevel(LOG_LEVEL_VERBOSE)
    
    
    def verbose_option(*param_decls, **attrs):
        def decorator(f):
            attrs.setdefault('is_flag', True)
            attrs.setdefault('callback', set_verbose)
            return click.option(*(param_decls or ('-v', '--verbose',)), **attrs)(f)
        return decorator
    
    
    def password_option(*param_decls, **attrs):
        def decorator(f):
            return click.option(*(param_decls or ('-p', '--password',)), **attrs)(f)
        return decorator
    
    
    click.verbose_option = verbose_option
    click.password_option = password_option


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
            'flags': flags,
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

