import click
from redis_trib.command import (
    create_cluster_command,
    info_cluster_command,
    check_cluster_command,
    add_node_command,
    delete_node_command,
    reshard_cluster_command,
    rebalance_cluster_command,
    fix_cluster_command,
    call_cluster_command,
    import_cluster_command,
)
from redis_trib.monkey_patch import patch_click_module


patch_click_module()


@click.group()
def cli():
    pass


@cli.command()
@click.argument('addrs', nargs=-1)
@click.option('-r', '--replicas', type=int, default=0)
@click.option('-c', '--user-custom', is_flag=True)
@click.option('-y', '--yes', 'yes', is_flag=True)
@click.password_option()
@click.verbose_option()
def create(addrs, verbose, password, replicas, user_custom, yes):
    create_cluster_command(addrs, password, replicas, user_custom, yes)


@cli.command()
@click.argument('addr')
@click.verbose_option()
@click.password_option()
def info(addr, verbose, password):
    info_cluster_command(addr, password)


@cli.command()
@click.argument('addr')
@click.verbose_option()
@click.password_option()
def check(addr, verbose, password):
    check_cluster_command(addr, password)


@cli.command()
@click.argument('new_addr')
@click.argument('addr')
@click.option('-m', '--master-id')
@click.option('-r', '--addr-as-master', is_flag=True)
@click.option('-s', '--slave', 'is_slave', is_flag=True)
@click.password_option()
@click.verbose_option()
def add_node(addr, new_addr, password, is_slave, master_id, addr_as_master):
    add_node_command(addr, new_addr, password, is_slave, master_id, addr_as_master)


@cli.command()
@click.argument('addr')
@click.argument('del_node_id')
@click.option('-r', '--rename-command', 'rename_commands', multiple=True)
@click.password_option()
@click.verbose_option()
def del_node(addr, del_node_id, password, rename_commands):
    delete_node_command(addr, del_node_id, password, rename_commands)


@cli.command()
@click.argument('addr')
@click.option('-f', '--from', 'from_ids')
@click.option('-t', '--to', 'to_id')
@click.option('--timeout', type=int, default=60)
@click.option('--pipeline', type=int, default=10)
@click.option('--slots', 'num_slots', type=int)
@click.option('-y', '--yes', 'yes', is_flag=True)
@click.verbose_option()
@click.password_option()
def reshard(addr, password, from_ids, to_id, pipeline, timeout, num_slots, yes):
    reshard_cluster_command(addr, password, from_ids, to_id,
            pipeline, timeout, num_slots, yes)

@cli.command()
@click.argument('addr')
@click.option('--weights')
@click.option('--use-empty-masters', is_flag=True)
@click.option('--pipeline', type=int, default=10)
@click.option('--timeout', type=int, default=60)
@click.option('--threshold', type=int, default=2)
@click.option('--simulate', is_flag=True)
@click.verbose_option()
@click.password_option()
def rebalance(addr, password, weights, use_empty_masters, pipeline, timeout, threshold, simulate):
    rebalance_cluster_command(addr, password, weights, use_empty_masters, 
            pipeline, timeout, threshold, simulate)


@cli.command()
@click.argument('addr')
@click.verbose_option()
@click.password_option()
def fix(addr, password):
    fix_cluster_command(addr, password)


@cli.command()
@click.argument('addr')
@click.argument('command', nargs=-1)
@click.verbose_option()
@click.password_option()
def call(addr, password, command):
    call_cluster_command(addr, password, *command)


@cli.command('import')
@click.argument('addr')
@click.option('--from-password')
@click.option('--from', 'from_addr')
@click.option('--replace', is_flag=True)
@click.option('--copy', is_flag=True)
@click.verbose_option()
@click.password_option()
def import_(addr, password, from_addr, from_password, replace, copy):
    import_cluster_command(addr, password, from_addr, from_password, replace, copy)


if __name__ == '__main__':
    cli()
