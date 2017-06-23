#!/usr/bin/env python3
import argparse
import os.path
import requests

import encodedcc

"""

Find orphaned ENCODE objects.

"""

# Define parent-child relationship.
orphans = [
    {'child': 'Library',
     'parent': 'Replicate'},
    {'child': 'Biosample',
     'parent': 'Library'},
    {'child': 'Donor',
     'parent': 'Biosample'},
    {'child': 'AnalysisStep',
     'parent': 'Pipeline'},
    {'child': 'Contruct',
     'parent': 'Any'},
    {'child': 'Document',
     'parent': 'Any'}
]

# Test development.
orphans = [{'child': 'Biosample',
            'child_field': 'accession',
            'parent': 'Library',
            'parent_field': 'biosample.accession'}]


def get_args():
    parser = argparse.ArgumentParser(description='__doc__')
    parser.add_argument('--type',
                        default=None,
                        help='Specify item type (or list of comma-separated'
                        ' item types in quotation marks) to check for orphans.'
                        ' Default is None.')
    parser.add_argument('--keyfile',
                        default=os.path.expanduser('~/keypairs.json'),
                        help='The keypair file. Default is {}.'
                        .format(os.path.expanduser('~/keypairs.json')))
    parser.add_argument('--key',
                        default='default',
                        help='The keypair identifier from the keyfile.'
                        ' Default is --key=default.')
    return parser.parse_args()


def get_accessions(item_type, field_name):
    accessions = []
    field_name_split = field_name.split('.')
    url = 'https://www.encodeproject.org/search/'\
          '?type={}&limit=all&format=json'\
          '&frame=embedded'.format(item_type)
    r = requests.get(url, auth=(key.authid, key.authpw))
    results = r.json()['@graph']
    print('Total {}: {}'.format(item_type, len(results)))
    for result in results:
        value = result
        for name in field_name_split:
            value = value.get(name, {})
        if isinstance(value, dict):
            continue
        accessions.append(value)
    return set(accessions)


def find_orphans(item):
    relationships = [x for x in orphans if x['child'] == item]
    for relation in relationships:
        child, parent = relation['child'], relation['parent']
        child_field = relation['child_field']
        parent_field = relation['parent_field']
        child_accessions = get_accessions(child, child_field)
        parent_accessions = get_accessions(parent, parent_field)
        same = child_accessions.intersection(parent_accessions)
        different = child_accessions.difference(parent_accessions)
        print("Number of {} with {}: {}".format(child,
                                                parent,
                                                len(same)))
        print("Number of {} without {}: {}".format(child,
                                                   parent,
                                                   len(different)))
        diff = iter(different)
        for i in range(5):
            print(next(diff))


def main():
    global key
    args = get_args()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    if args.type is None:
        item_type = [x['child'] for x in orphans]
        print('Default orphan search:')
        print(*['{} not associated with {}'.format(x['child'], x['parent'])
                for x in sorted(orphans, key=lambda x: x['child'])], sep="\n")
    else:
        item_type = [x.strip().capitalize() for x in args.type.split(',')]
        print('Orphan search:')
        print(*['{} not associated with {}'.format(x['child'], x['parent'])
                for x in sorted(orphans, key=lambda x: x['child'])
                if x['child'] in item_type], sep="\n")

    [find_orphans(item) for item in item_type]

    #[find_orphans(item) for item in item_type]


if __name__ == '__main__':
    main()
