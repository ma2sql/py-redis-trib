import click
from redis_trib.command import (
    create_cluster_command,
    info_cluster_command,
    check_cluster_command,
    add_node_command,
    delete_node_command,
)


@click.group()
def cli():
    pass


@cli.command()
@click.argument('addrs', nargs=-1)
@click.option('-p', '--password')
@click.option('-r', '--replicas', type=int, default=0)
@click.option('-c', '--user-custom', is_flag=True)
def create(addrs, password, replicas, user_custom):
    create_cluster_command(addrs, password, replicas, user_custom)


@cli.command()
@click.argument('addr')
@click.option('-p', '--password')
def info(addr, password):
    info_cluster_command(addr, password)


@cli.command()
@click.argument('addr')
@click.option('-p', '--password')
def check(addr, password):
    check_cluster_command(addr, password)


@cli.command()
@click.argument('new_addr')
@click.argument('addr')
@click.option('-p', '--password')
@click.option('-m', '--master-id')
@click.option('-r', '--addr-as-master', is_flag=True)
@click.option('-s', '--slave', 'is_slave', is_flag=True)
def add_node(addr, new_addr, password, is_slave, master_id, addr_as_master):
    add_node_command(addr, new_addr, password, is_slave, master_id, addr_as_master)


@cli.command()
@click.argument('addr')
@click.argument('del_node_id')
@click.option('-p', '--password')
@click.option('-r', '--rename-command', 'rename_commands', multiple=True)
def del_node(addr, del_node_id, password, rename_commands):
    delete_node_command(addr, del_node_id, password, rename_commands)


if __name__ == '__main__':
    cli()
