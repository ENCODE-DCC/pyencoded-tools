import click
import urllib.parse


@click.command()
@click.argument('encode_objects',
                nargs=-1,
                required=True)
@click.option('--json/--no-json', default=False)
@click.pass_obj
def launch(ctx, encode_objects, json):
    '''
    Launch ENCODE objects in web browser.
    '''
    click.secho('Launching on server: {}'.format(ctx.connection.server))
    for encode_object in encode_objects:
        url = urllib.parse.urljoin(ctx.connection.server, encode_object)
        if json:
            url = url + '/?format=json'
        click.launch(url)
