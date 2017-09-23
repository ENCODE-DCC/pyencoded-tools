import click
import urllib.parse


@click.command()
@click.argument('encode_object')
@click.option('--json/--no-json', default=False)
@click.pass_obj
def launch(ctx, encode_object, json):
    '''
    Launch ENCODE object in web browser.
    '''
    click.secho('Launching on server: {}'.format(ctx.connection.server))
    url = urllib.parse.urljoin(ctx.connection.server, encode_object)
    if json:
        url = urllib.parse.urljoin(url, '/?format=json')
    click.launch(url)
