import click
import json
import os
import pandas as pd
import uuid

from ..encode_utils import grab

from collections import Counter, defaultdict
from . import encodedcc, explore
from ..output import print_header, print_data, print_error


def build_path(path, new_value, data):
    if isinstance(new_value, int) and len(split_dot(path)) < 2:
        try:
            possible_path = data.get('accession',
                                     data.get('@id',
                                              data.get('uuid', None)))
            if possible_path is not None:
                new_value = possible_path
        except AttributeError:
            pass
    new_path = '{}.'.format(new_value)
    return new_path if path is None else path + new_path


def top_level(d):
    top = d.get('accession', d.get('@id', d.get('uuid', None)))
    if top is not None:
        return '{}.'.format(top)
    return top


def flatten_json(data):
    flattened = {}

    def flatten(x, path=None):
        if isinstance(x, dict):
            [flatten(v, build_path(path, k, v)) for k, v in x.items()]
        elif isinstance(x, list):
            if len(x) == 0:
                flattened[path[:-1]] = x
            else:
                [flatten(y, build_path(path, i, y)) for i, y in enumerate(x)]
        else:
            path = path[:-1] if path.endswith('.') else path
            flattened[path] = x
    if isinstance(data, list):
        [flatten(d, top_level(d)) for d in data]
    else:
        flatten(data, top_level(data))
    return flattened


def make_int(x):
    try:
        return int(x)
    except:
        return x


def make_list(x):
    try:
        return json.loads(x)
    except:
        return x


def sort_num(x):
    return tuple([int(d) if isinstance(make_int(d), int) else 0 for d in x])


def sort_alpha(x):
    return tuple([d if not isinstance(make_int(d), int) else 'z' for d in x])


def sort_last(x):
    last_number = [int(d) if isinstance(make_int(d),
                                        int) else 0 for d in x][:-1]
    return last_number


def sort_flattened(flat, return_data=False):
    k = sorted(flat, key=lambda x: sort_num(x))
    s = sorted(k, key=lambda x: sort_alpha(x))
    f = sorted(s, key=lambda x: sort_last(x))
    data = f
    if return_data:
        data = [(x, flat[x]) for x in f]
    return data


def make_df(sorted_flat):
    df = pd.DataFrame(sorted_flat)
    df['object_id'] = df[0].apply(lambda x: x.split('.')[0])
    df['path'] = df[0].apply(lambda x: '.'.join(x.split('.')[1:]))
    df = df.rename(columns={1: 'value'})
    df = df[['object_id', 'path', 'value']]
    return df


def pivot_df(df):
    return df.pivot(columns='path', index='object_id')['value']


def build_default_dict(df):
    dd = defaultdict(dict)
    for x in df.reset_index(drop=True).to_dict(orient='records'):
        dd[x['object_id']][x['path']] = make_list(x['value'])
    return dd


def split_dot(x):
    return x.split('.')


def build_list(parent, data):
    return [v for k, v in data.items() if '.'.join(split_dot(k)[:-1]) == parent]


def build_dict(parent, data):
    return {split_dot(k)[-1:][0]: v for k, v in data.items() if '.'.join(split_dot(k)[:-1]) == parent}


def is_part_of_list(child):
    if isinstance(make_int(child), int):
        return True
    return False


def is_part_of_dict(parent):
    next_level = split_dot(parent)[-1:][0]
    if isinstance(make_int(next_level), int):
        return True
    return False


def process_list(data, collapse=False, sublist=None):
    new_data = {}
    for k, v in data.items():
        path_split = split_dot(k)
        parent, child = '.'.join(path_split[:-1]), path_split[-1:][0]
        if parent:
            if collapse:
                if k not in sublist:
                    new_data[k] = v
                    continue
                new_data[parent] = build_dict(parent, data)
            elif parent not in new_data and k not in new_data:
                if is_part_of_list(child):
                    new_data[parent] = build_list(parent, data)
                elif is_part_of_dict(parent):
                    new_data[parent] = build_dict(parent, data)
                else:
                    new_data[k] = v
            else:
                pass
        else:
            new_data[k] = v
    return new_data


def get_max_path_length(data):
    return max([len(split_dot(x)) for x in data.keys()])


def filter_max_path_length(data, length):
    return [x for x in data.keys() if len(split_dot(x)) == length]


def group(old):
    count = 0
    while True:
        count += 1
        new = process_list(old)
        if new == old:
            break
        old = {k: v for k, v in new.items()}
    return (new, new == old)


def collapse(new):
    sublist = filter_max_path_length(new, get_max_path_length(new))
    old = process_list(new, collapse=True, sublist=sublist)
    return (old, old == new)


def build_json(flat_data):
    old = flat_data
    while True:
        new, change1 = group(old)
        old, change2 = collapse(new)
        if change1 and change2:
            break
    return old


def pull_identifier(f):
    return f.get('@id', f.get('accession', f.get('uuid')))


def match_path(path, f):
    split_path = split_dot(path)
    split_filter = split_dot(f)
    split_path_clean = [
        p for p in split_path if not isinstance(make_int(p), int)
    ]
    for p, f in zip(split_path_clean, split_filter):
        if f == '*':
            continue
        elif p != f:
            return False
    return True


def filter_path(filters, flat_data):
    filtered_data = []
    for f in filters:
        filtered = [x for x in flat_data if match_path(x[0], f)]
        if filtered:
            filtered_data.extend(filtered)
    return filtered_data


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
@click.option('--outfile',
              default=os.path.expanduser('~/Desktop/flat_output.xlsx'),
              type=click.Path(),
              help='Excel file for flattened ouput.')
@click.option('--infile',
              default=os.path.expanduser('~/Desktop/flat_output.xlsx'),
              type=click.Path(),
              help='Flattened Excel file.')
@click.option('--save/--no-save',
              default=False,
              help='Save flattened output to Excel.')
@click.option('--load/--no-load',
              default=False,
              help='Load flattened output to Excel.')
@click.option('--get_associated/--no-get_associated',
              default=False,
              help='Get associated objects using --related_field.')
@click.option('--related_field',
              default=None,
              help='For use with --get_associated.')
@click.option('--related_object',
              default=None,
              help='For use with --get_associated.')
@click.option('--filt', '-f',
              multiple=True, default=['all'])
@click.pass_obj
def model(ctx, encode_object, search_type, field, get_associated,
          related_field, related_object, limit, frame, where, save,
          load, outfile, infile, filt):
    '''
    Flatten or build ENCODE metadata.
    '''
    if load:
        print('Loading {}'.format(infile))
        df = pd.read_excel(infile)
        if 'object_id' not in df.columns:
            df['object_id'] = [str(uuid.uuid4()) for x in df.iloc[:, 0].values]
        unpivot_df = df.melt(id_vars='object_id', var_name='path').dropna()
        default_dict = build_default_dict(unpivot_df)
        json_objects = []
        for k, v in default_dict.items():
            json_objects.append(build_json(v))
        if search_type is not None:
            click.secho(search_type, bold=True, fg='green')
        for i, nested in enumerate(json_objects, 1):
            click.secho('*OBJECT {}*\n------------'.format(i),
                        bold=True,
                        fg='blue')
            print(json.dumps(nested, indent=4, sort_keys=True))
        if save:
            json_path = os.path.expanduser('~/Desktop/output_objects.json')
            with open(json_path, 'w') as f:
                json.dump(json_objects, f, sort_keys=True, indent=4)
    else:
        click.secho('Using server: {}'.format(ctx.connection.server))
        encode_object = explore.check_inputs(encode_object, search_type, where)
        response = explore.get_data(ctx, encode_object, limit, frame)
        if get_associated:
            assert related_field is not None and related_object is not None
            grab.associated_search = grab.make_associated_url(
                ctx.connection.server)
            related_ids = [pull_identifier(f) for f in response]
            session = grab.create_session()
            response = grab.get_associated(
                related_object,
                related_field,
                related_ids,
                session=session
            )
            session.close()
        if response:
            data = flatten_json(response)
            sorted_flat = sort_flattened(data, return_data=True)
            if 'all' not in filt:
                sorted_flat = filter_path(filt, sorted_flat)
            if sorted_flat:
                df = make_df(sorted_flat)
                default_dict = build_default_dict(df)
                print(*sorted_flat, sep='\n')
            else:
                print('No results found')
        if save:
            print('Saving to {}'.format(outfile))
            pivoted_df = pivot_df(df)
            sorted_df = pivoted_df[sort_flattened(pivoted_df.columns)]
            sorted_df.to_excel(outfile)
