import click

from collections import Counter
from . import encodedcc


def objects_with_key(key, data):
    return [f for f in data if f.get(key) is not None]


def raise_error(key, field):
    raise click.BadParameter(click.style(
        '{} not found'.format(key), bold=True, fg='red'), param_hint=field)


def parse_where(where):
    return '&' + '&'.join([w.strip() for w in where.split(',')])


def crawl(*args, **kwargs):
    items_found = []

    def crawl_object(data, fieldsplit):
        '''
        Recursively crawl JSON object.
        '''
        if isinstance(data, list):
            [crawl_object(d, fieldsplit) for d in data]
            return
        if not fieldsplit:
            items_found.append(data)
            return
        current_field = fieldsplit[0]
        if isinstance(data, dict):
            if current_field == '*':
                fieldsplit.pop(0)
                [crawl_object(data[k], fieldsplit) for k in data.keys()]
                fieldsplit.insert(0, current_field)
                return
            try:
                data = data[current_field]
            except KeyError:
                return
            fieldsplit.pop(0)
            crawl_object(data, fieldsplit)
            fieldsplit.insert(0, current_field)
            return
        raise_error(fieldsplit, '--field')
    crawl_object(*args, **kwargs)
    return items_found


@click.command()
@click.argument('encode_object',
                default='/profiles/')
@click.option('--search_type',
              default=None,
              help='Type of ENCODE object as listed in /profiles/.')
@click.option('--where',
              default=None,
              required=False,
              help='Filter search_type results using "fieldname1=value2,'
              ' fieldname2=value2" format.')
@click.option('--field',
              default=None,
              help='Field to return. List subfields with dot notation: '
              'replicates.antibody.lot_reviews.biosample_term_id')
@click.option('--limit',
              default='all',
              help='Number of results to return in search. Default is all.')
@click.option('--frame',
              default='object',
              type=click.Choice(['object', 'embedded']),
              help='JSON returned as regular object or embedded object.'
              ' Default is object.')
@click.option('--count/--no-count',
              default=False,
              help='Return count of items.')
@click.pass_obj
def explore(ctx, encode_object, search_type, field, limit, frame, count, where):
    '''
    Explore facets of ENCODE metadata.
    '''
    click.secho('Using server: {}'.format(ctx.connection.server))
    object_default = '/profiles/'
    if search_type is not None and encode_object == object_default:
        encode_object = '/search/?type={}'.format(search_type)
        if where is not None:
            encode_object = encode_object + parse_where(where)
    if where is not None and search_type is None:
        raise_error('--search_type required for --where', 'where')
    response = encodedcc.get_ENCODE(encode_object,
                                    ctx.connection,
                                    limit=limit,
                                    frame=frame)
    if response.get('code', 200) != 200:
        raise_error(encode_object, 'encode_object')
    # Expose results if reponse from search.
    try:
        response = response['@graph']
    except KeyError:
        pass
    fieldsplit = field.split('.') if field is not None else []
    data = crawl(response, fieldsplit)
    if data:
        click.secho('Found {} {}{}{}'.format(len(data),
                                             encode_object,
                                             ':' if field else ' ',
                                             field if field else ''),
                    bold=True, fg='green')
        if all([isinstance(d, dict) for d in data]):
            click.secho('(Keys)', bold=True)
            keys = list(set([x for d in data for x in d.keys()]))
            key_types = [type(objects_with_key(k, data)[
                              0][k]).__name__ for k in keys]
            output = ['{} ({})'.format(k, t) for k, t in zip(keys, key_types)]
            click.secho(
                '\n'.join(sorted(output)), bold=True)
        else:
            if count:
                data = sorted([(k, v) for k, v in Counter(
                    data).items()], key=lambda x: x[1], reverse=True)
            click.secho('\n'.join([str(d) for d in data]), bold=True)
    else:
        click.secho('No results found', bold=True, fg='red')
