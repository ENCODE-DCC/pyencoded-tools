import click
import json
import os

from . import encodedcc


@click.command()
@click.argument('encode_object')
@click.option('--field', default=None, help='Field to return.')
def get(encode_object, field):
    '''
    GETs ENCODE metadata given accession/UUID/alias.
    '''
    key = encodedcc.ENC_Key(os.path.expanduser('~/keypairs.json'), 'test')
    connection = encodedcc.ENC_Connection(key)
    response = encodedcc.get_ENCODE(encode_object, connection)
    if field is not None:
        response = response[field]
    click.echo(json.dumps(response, indent=4, sort_keys=True))
