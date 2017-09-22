import click
import json
import os

from . import encodedcc


@click.command()
@click.argument('encode_object')
@click.option('--field', default=None, help='Field to return.')
@click.pass_obj
def get(ctx, encode_object, field):
    '''
    GETs ENCODE metadata given accession/UUID/alias.
    '''
    click.secho('Using server: {}'.format(ctx.connection.server))
    response = encodedcc.get_ENCODE(encode_object, ctx.connection)
    if field is not None:
        response = response[field]
    click.echo(json.dumps(response, indent=4, sort_keys=True))
