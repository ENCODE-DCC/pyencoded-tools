import click

from .commands.get import get


@click.group()
def cli():
    '''
    Entry point for CLI.
    '''
    pass


cli.add_command(get)
