#!/usr/bin/env python3
import argparse
import os.path
import pandas as pd
import requests

import encodedcc

"""

Find orphaned ENCODE objects.

"""

# Define parent-child relationship.
orphans = [{'child': 'Biosample',
            'child_field': 'accession',
            'parent': 'Library',
            'parent_field': 'biosample.accession'},
           {'child': 'Donor',
            'child_field': 'accession',
            'parent': 'Biosample',
            'parent_field': 'donor.accession'},
           {'child': 'Library',
            'child_field': 'accession',
            'parent': 'Replicate',
            'parent_field': 'library.accession'},
           {'child': 'AnalysisStep',
            'child_field': 'uuid',
            'parent': 'Pipeline',
            'parent_field': 'analysis_steps.uuid'},
           {'child': 'Construct',
            'child_field': '@id',
            'parent': ['Biosample',
                       'ConstructCharacterization'],
            'parent_field': ['constructs.@id',
                             'constructs.@id']
            },
           {'child': 'Document',
            'child_field': '@id',
            'parent':  ['AnalysisStep',
                        'Annotation',
                        'AntibodyCharacterization',
                        'Biosample',
                        'BiosampleCharacterization',
                        'Characterization',
                        'Construct',
                        'ConstructCharacterization',
                        'Crispr',
                        'Dataset',
                        'Donor',
                        'DonorCharacterization',
                        'Experiment',
                        'FileSet',
                        'FlyDonor',
                        'GeneticModification',
                        'GeneticModificationCharacterization',
                        'HumanDonor',
                        'Library',
                        'MatchedSet',
                        'ModificationTechnique',
                        'MouseDonor',
                        'OrganismDevelopmentSeries',
                        'Pipeline',
                        'Project',
                        'PublicationData',
                        'Reference',
                        'ReferenceEpigenome',
                        'ReplicationTimingSeries',
                        'RNAi',
                        'Series',
                        'Treatment',
                        'TreatmentConcentrationSeries',
                        'TreatmentTimeSeries',
                        'UcscBrowserComposite',
                        'WormDonor',
                        'File'],
            'parent_field': ['documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'documents.@id',
                             'file_format_specification.@id']}]


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
    childless_parents = []
    field_name_split = field_name.split('.')
    url = 'https://www.encodeproject.org/search/'\
          '?type={}&limit=all&format=json'\
          '&frame=embedded'.format(item_type)
    r = requests.get(url, auth=(key.authid, key.authpw))
    results = r.json()['@graph']
    print('Total {}: {}'.format(item_type, len(results)))
    for result in results:
        value = result
        values = None
        for name in field_name_split:
            # Deal with list of objects.
            if isinstance(value, list):
                values = [x.get(name) if isinstance(x, dict)
                          else x for x in value]
                value = None
                continue
            value = value.get(name, {})
        # Deal with objects missing field.
        if isinstance(value, dict):
            childless_parents.append(result.get('accession',
                                                result.get('uuid')))
            continue
        # See if replacement is also orphaned (only for child objects).
        if ((result.get('status') == 'replaced')
                and (len(field_name_split) == 1)):
            url = 'https://www.encodeproject.org/'\
                  '{}/?format=json'.format(result.get('accession',
                                                      result.get('uuid',
                                                                 result.get('@id'))))
            r = requests.get(url, auth=(key.authid, key.authpw))
            if (r.status_code == 200):
                r = r.json()
                value = r.get('accession',
                              r.get('uuid',
                                    r.get('@id')))
        # Deal with values recovered from a list of objects/strings.
        if values is not None:
            if len(values) == 0:
                childless_parents.append(result.get('accession',
                                                    result.get('uuid')))
            else:
                accessions.extend(values)
            continue
        accessions.append(value)
    if childless_parents:
        print('Number of {} missing {} field'
              ' (childless parents): {}'.format(item_type,
                                                field_name,
                                                len(childless_parents)))
        print('Sample childless parents:')
        print(*childless_parents[:min([5, len(childless_parents)])], sep='\n')
    return set(accessions)


def find_orphans(i, item):
    relationships = [x for x in orphans if x['child'].lower() == item]
    for relation in relationships:
        child, parent = relation['child'], relation['parent']
        print('{}. {} not associated with {}'.format((i + 1), child, parent))
        child_field = relation['child_field']
        parent_field = relation['parent_field']
        child_accessions = get_accessions(child, child_field)
        # Deal with multiple parents.
        if isinstance(parent, list):
            parent_accessions = set()
            for p, f in zip(parent, parent_field):
                accessions = get_accessions(p, f)
                parent_accessions = parent_accessions.union(accessions)
        else:
            parent_accessions = get_accessions(parent, parent_field)
        same = child_accessions.intersection(parent_accessions)
        different = child_accessions.difference(parent_accessions)
        print('Number of {} with {} (families): {}'.format(child,
                                                           parent,
                                                           len(same)))
        print('Sample families:')
        sim = iter(same)
        for i in range(min([5, len(same)])):
            print(next(sim))
        print('Number of {} without {} (orphans): {}'.format(child,
                                                             parent,
                                                             len(different)))
        print('Sample orphans:')
        diff = iter(different)
        for i in range(min([5, len(different)])):
            print(next(diff))
        return (child_accessions, parent_accessions)


def main():
    global key
    global args
    args = get_args()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    if args.type is None:
        item_type = [x['child'].lower() for x in orphans]
        print('Default orphan search:')
    else:
        input_type = [x.strip().lower() for x in args.type.split(',')]
        item_type = [x for x in input_type
                     if x in [x['child'].lower() for x in orphans]]
        if len(input_type) != len(item_type):
            raise ValueError("Invalid item type: {}.".format((set(input_type)
                                                              - set(item_type))))
        print('Custom orphan search:')
    return zip(list(item_type),
               [find_orphans(i, item) for i, item in enumerate(item_type)])


if __name__ == '__main__':
    results = main()
