

def patch_click_module():
    import click

    def set_verbose(ctx, param, value):
        if value:
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


