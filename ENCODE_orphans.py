#!/usr/bin/env python3
import argparse
import os.path
import pandas as pd
import requests

import encodedcc

"""

Purpose
-------
Find orphaned ENCODE objects.

Example
-------
A Biosample with no Libraries means that a Biosample
was created and then never referred to by a Library.
The Biosample is orphaned.

Method
-------
This script gathers all accession/uuids of child objects
and all accession/uuids of child objects embedded in parent
objects and reports the difference.

Tests
------
Tests are in /pyencoded-tools/tests/test_encode_orphans.py and can be run
with pytest at the command line.

"""

# Define parent-child relationship:
# A child is the possible orphan while a parent is any object that could refer
# to it (as defined in the schema at https://www.encodeproject.org/profiles/).
# The child can have one parent or multiple parents (e.g. Documents). To
# define an orphan, add a new object to the 'orphans' list, where 'child' is
# the possible orphan object, 'child_field' is its identification field
# (accession/uuid/@id), 'parent' is the parent object (or list of parent
# objects), and 'parent_field' is the child's identification field
# (accession/uuid/@id) in the parent (a list is required if there are multiple
# parents, and the 'parent' list and 'parent_field' list must be same length.
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
                             'file_format_specification.@id']}]


def get_id_type(value):
    """Returns field_name of identifier.
    """
    if "/" in value:
        return '@id'
    elif len(value) == 11:
        return 'accession'
    elif len(value) == 36:
        return 'uuid'
    else:
        raise ValueError('Invalid identifier')


def extract_profile(value, results):
    """Returns identifier, status, lab, date_created, submitted_by, and @type
    of child object given accession (or other identifier) and results from GET
    request.
    """
    na = 'not_available'
    id_field = get_id_type(value)
    data = [{'accession': value,
             'status': r.get('status', na),
             'lab': r.get('lab', {})
             if isinstance(r.get('lab', {}), str)
             else r.get('lab', {}).get('name', na),
             'date_created': r.get('date_created', na),
             'submitted_by': r.get('submitted_by', {})
             if isinstance(r.get('submitted_by', {}), str)
             else r.get('submitted_by', {}).get('title', na),
             '@type': r.get('@type', [])[:1]}
            for r in results if r.get(id_field) == value][0]
    return data


def find_replacement(value):
    """Returns replacement identifier (accession/uuid/@id) given replaced
    identifier if it exists.
    """
    url = 'https://www.encodeproject.org/'\
          '{}/?format=json'.format(value)
    r = requests.get(url, auth=(key.authid, key.authpw))
    if (r.status_code == 200):
        r = r.json()
        # Set replaced accession to replacement accession.
        id_field = get_id_type(value)
        new_value = r.get(id_field)
        return new_value
    else:
        return value


def get_data(item_type):
    """Returns results of search for specified ENCODE item type.
    """
    # Grab all objects of type item_type.
    url = 'https://www.encodeproject.org/search/'\
        '?type={}&limit=all&format=json'\
        '&frame=embedded'.format(item_type)
    r = requests.get(url, auth=(key.authid, key.authpw))
    results = r.json()['@graph']
    print('Total {}: {}'.format(item_type, len(results)))
    return results


def parse_data(results, field_name):
    """Returns DataFrame of extracted identifier values, as well as list of
    objects that are missing identifiers (childless parents), given results
    from GET query and field_name of identifier (e.g. 'accession' or
    'analysis_step.uuid').
    """
    data = []
    childless_parents = []
    field_name_split = field_name.split('.')
    # Process each object one at a time.
    for result in results:
        value, values = result, None
        # Access value in nested fields if required.
        for name in field_name_split:
            # Deal with list of objects.
            if isinstance(value, list):
                # Recover all values in a list.
                values = [x.get(name) if isinstance(x, dict)
                          else x for x in value]
                # Set singluar value to None because
                # now have multiple values.
                value = None
                # Recovered values so exit loop.
                break
            # Extract values from list of dictionaries.
            # Otherwise access field name and update value.
            value = value.get(name, {})
        # Deal with objects missing field. An empty dict is returned if field
        # not present.
        if isinstance(value, dict):
            childless_parents.append({'accession': result.get('accession',
                                                              result.get('uuid'))})
            continue
        # See if replacement is also orphaned (only for child objects).
        if ((result.get('status') == 'replaced')
                and (len(field_name_split) == 1)):
            value = find_replacement(value)
        # Deal with values recovered from a list of objects/strings.
        if values is not None:
            # Possible to have empty list or list of Nones if field_name
            # not found.
            if ((len(values) == 0) or (None in values)):
                childless_parents.append({'accession': result.get('accession',
                                                                  result.get('uuid'))})
            else:
                # Add recovered values to data list.
                for value in values:
                    data.append({'accession': value})
            continue
        # Add recovered value to data list.
        # Extract metadata only for child objects.
        if len(field_name_split) == 1:
            data.append(extract_profile(value, results))
        # For parent objects just add accession of child object.
        else:
            data.append({'accession': value})
    return pd.DataFrame(data), childless_parents


def find_orphans(i, item):
    """Returns orphans of specified child item."""
    orphan_data = []
    # Grab all relationships in 'orphans' list with specified child.
    relationships = [x for x in orphans if x['child'].lower() == item]
    for relation in relationships:
        child, parent, child_field, parent_field = (relation['child'],
                                                    relation['parent'],
                                                    relation['child_field'],
                                                    relation['parent_field'])
        print('{}. {} not associated with {}'.format((i + 1), child, parent))
        # Grab and parse child data.
        child_results = get_data(child)
        child_data, _ = parse_data(child_results, child_field)
        # Grab and parse parent data.
        # If multiple parents.
        if isinstance(parent, list):
            # Build list from multiple parents.
            parent_data = []
            childless_parents = []
            assert len(parent) == len(parent_field)
            for p, f in zip(parent, parent_field):
                p_results = get_data(p)
                p_data, cp = parse_data(p_results, f)
                parent_data.append(p_data)
                if len(cp) != 0:
                    childless_parents.extend(cp)
            # Combine DataFrames
            parent_data = pd.concat(parent_data)
        else:
            # If only single parent.
            parent_results = get_data(parent)
            parent_data, childless_parents = parse_data(parent_results,
                                                        parent_field)
        # Compare groups.
        same = child_data[child_data.accession.isin(parent_data.accession)]\
            .drop_duplicates('accession')
        diff = child_data[~(child_data.accession.isin(parent_data.accession))]\
            .drop_duplicates('accession')
        if childless_parents:
            print('Number of {} missing {} field'
                  ' (childless parents): {}'.format(parent,
                                                    parent_field,
                                                    len(childless_parents)))
        print('Number of {} with {} (families): {}'.format(child,
                                                           parent,
                                                           len(same)))
        print('Number of {} without {} (orphans): {}'.format(child,
                                                             parent,
                                                             len(diff)))
        orphan_data.append((child, parent, diff.reset_index(drop=True)))
    return orphan_data


def get_args():
    parser = argparse.ArgumentParser(description='__doc__')
    parser.add_argument('--type',
                        default=None,
                        help='Specify item type (or list of comma-separated'
                        ' item types in quotation marks) to check for orphans.'
                        ' Default is None.')
    parser.add_argument('--outfile',
                        default=None,
                        help='Outfile for TSV of orphans. Results only printed'
                        ' to screen by default.')
    parser.add_argument('--keyfile',
                        default=os.path.expanduser('~/keypairs.json'),
                        help='The keypair file. Default is {}.'
                        .format(os.path.expanduser('~/keypairs.json')))
    parser.add_argument('--key',
                        default='default',
                        help='The keypair identifier from the keyfile.'
                        ' Default is --key=default.')
    return parser.parse_args()


def main():
    global key
    args = get_args()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    # Build list of ENCODE object types for orphan checking.
    if args.type is None:
        # Default list.
        item_type = [x['child'].lower() for x in orphans]
        print('Default orphan search:')
    else:
        # User-defined list.
        input_type = [x.strip().lower() for x in args.type.split(',')]
        item_type = [x for x in input_type
                     if x in [x['child'].lower() for x in orphans]]
        # Check for invalid --type input from user.
        if len(input_type) != len(item_type):
            raise ValueError("Invalid item type: {}.".format((set(input_type)
                                                              - set(item_type))))
        print('Custom orphan search:')
    results = [find_orphans(i, item) for i, item in enumerate(item_type)]
    # Make flat.
    results = [item for sublist in results for item in sublist]
    for result in results:
        df = result[2]
        df.date_created = df['date_created'].apply(lambda x: pd.to_datetime(x))
        print(df[['accession',
                  'status',
                  'lab',
                  'date_created']].sort_values(by='date_created',
                                               ascending=False).reset_index(drop=True).to_string())
    if args.outfile is not None:
        df = pd.concat([result[2] for result in results])
        df.date_created = df.date_created.apply(lambda x: pd.to_datetime(x))
        df['@type'] = df['@type'].apply(lambda x: x[0])
        df.sort_values(by=['@type', 'date_created'],
                       ascending=[True, False]).reset_index(drop=True).to_csv(args.outfile,
                                                                              sep='\t')


if __name__ == '__main__':
    main()
