import click
import os

from .commands.get import get
from .commands.explore import explore
from .commands.launch import launch
from .auth import Auth


@click.group()
@click.option('--keyfile',
              default=os.path.expanduser('~/keypairs.json'),
              type=click.Path(exists=True),
              help='The JSON file containing credentials.')
@click.option('--key',
              default='prod',
              help='The keypair identifier from keyfile')
@click.pass_context
def cli(ctx, keyfile, key):
    '''
    Command-line tool for ENCODE project.
    '''
    ctx.obj = Auth(keyfile, key)


cli.add_command(get)
cli.add_command(explore)
cli.add_command(launch)
