#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

import encode_utils.connection as euc

# and treatment duration & amount & duration_units & amount_units? just
# treaments - match whole list - but can't query that way easily
# figure out how to handle life_stages - if anything other than unknown
# present, use that (and query also unknown) - what if 2 non-unknown present?

CONN = euc.Connection('prod', dry_run=True)

##################################
exp_param = [
    ('type', 'Experiment'),
    ('status', 'released'),
    # ('status', 'submitted'),
    # ('status', 'in progress'),
    ('internal_status!', 'pipeline error'),
    ('award.project', 'ENCODE'),
    ('field', 'accession'),
    ('field', 'assay_term_name'),
    ('field', 'target'),
    ('field', 'biosample_ontology')
]

chip_param = [
    ('assay_term_name', 'ChIP-seq')
]

other_param = [
    ('assay_term_name', 'DNase-seq'),
    ('assay_term_name', 'ATAC-seq')
]

mouse_adds = [
    (
        'replicates.library.biosample.donor.organism.scientific_name',
        'Mus musculus'
    ),
    ('field', 'replicates.library.biosample.age_display'),
    ('field', 'replicates.library.biosample.treatments'),
    ('field', 'replicates.library.biosample.donor'),
    ('field', 'replicates.library.biosample.subcellular_fraction_term_name')
]

human_adds = [
    (
        'replicates.library.biosample.donor.organism.scientific_name',
        'Homo sapiens'
    ),
    ('field', 'replicates.library.biosample.treatments'),
    ('field', 'replicates.library.biosample.donor'),
    ('field', 'replicates.library.biosample.donor.life_stage'),
    ('field', 'replicates.library.biosample.subcellular_fraction_term_name')
]
##################################


def create_SEs():
    mouse_exp_param = (exp_param + chip_param + other_param + mouse_adds).copy()
    results = CONN.search(list(mouse_exp_param))
    mouse_sets = {}
    for obj in results:
        assay = obj['assay_term_name']
        accession = obj['accession']
        if 'target' in obj:
            target = obj['target']['label']
        else:
            target = 'none'
        biosample = obj['biosample_ontology']['name']
        one_bio_obj = obj['replicates'][0]['library']['biosample']
        donor = obj['replicates'][0]['library']['biosample']['donor']['accession']
        age = one_bio_obj.get('age_display', 'unknown')
        treatment = 'no treatment'
        if len(one_bio_obj['treatments']) > 0:
            treatment = one_bio_obj['treatments'][0]['treatment_term_name']
        fraction = one_bio_obj.get(
            'subcellular_fraction_term_name', 'no fraction'
        )
        combination = (biosample, age, treatment, fraction, donor)
        mouse_sets.setdefault(combination, {})
        if target in ['H3K27me3', 'H3K36me3', 'H3K4me1', 'H3K4me3', 'H3K27ac', 'H3K9me3', 'CTCF', 'POLR2A', 'EP300']:
            mouse_sets[combination].setdefault(target, [])
            mouse_sets[combination][target].append(accession)
        if assay in ['DNase-seq', 'ATAC-seq']:
            mouse_sets[combination].setdefault(assay, [])
            mouse_sets[combination][assay].append(accession)

    for combination in mouse_sets:
        if len(mouse_sets[combination].keys()) > 5:
            btype_obj = CONN.get('/biosample-types/{}/'.format(combination[0]))
            print(
                ' '.join([
                    str(combination[1]),
                    btype_obj['term_name'],
                    btype_obj['classification'],
                    combination[2],
                    'treated',
                    combination[3],
                    'fraction',
                    combination[4],
                    'donor'
                ])
            )
            print(combination[0])
            print(mouse_sets[combination])
            print('-----------------------')

    ##################################
    human_exp_param = (exp_param + chip_param + other_param + human_adds).copy()
    results = CONN.search(list(human_exp_param))
    human_sets = {}
    for obj in results:
        assay = obj['assay_term_name']
        accession = obj['accession']
        if 'target' in obj:
            target = obj['target']['label']
        else:
            target = 'none'
        biosample = obj['biosample_ontology']['name']
        one_bio_obj = obj['replicates'][0]['library']['biosample']
        donor = obj['replicates'][0]['library']['biosample']['donor']['accession']

        life_stage = tuple(
            sorted(
                set(
                    replicate['library']['biosample'].get('donor', {}).get(
                        'life_stage', 'unknown'
                    )
                    for replicate in obj['replicates']
                )
            )
        )

        treatment = 'no treatment'
        if len(one_bio_obj['treatments']) > 0:
            treatment = one_bio_obj['treatments'][0]['treatment_term_name']
        fraction = one_bio_obj.get(
            'subcellular_fraction_term_name', 'no fraction'
        )

        combination = (biosample, life_stage, treatment, fraction, donor)
        human_sets.setdefault(combination, {})
        if target in ['H3K27me3', 'H3K36me3', 'H3K4me1', 'H3K4me3', 'H3K27ac', 'H3K9me3', 'CTCF', 'POLR2A', 'EP300']:
            human_sets[combination].setdefault(target, [])
            human_sets[combination][target].append(accession)
        if assay in ['DNase-seq', 'ATAC-seq']:
            human_sets[combination].setdefault(assay, [])
            human_sets[combination][assay].append(accession)

    for combination in human_sets.keys():
        if len(human_sets[combination].keys()) > 5:
            btype_obj = CONN.get('/biosample-types/{}/'.format(combination[0]))
            print(
                ' '.join([
                    str(combination[1]),
                    btype_obj['term_name'],
                    btype_obj['classification'],
                    combination[2],
                    'treated',
                    combination[3],
                    'fraction',
                    combination[4],
                    'donor'
                ])
            )
            print(combination[0])
            print(human_sets[combination])
            print('-----------------------')


def get_parser():
    parser = argparse.ArgumentParser(
        description='Script to find experiments for Segway pipeline '
        'the ENCODE portal'
    )
    subparsers = parser.add_subparsers()
    parser_create = subparsers.add_parser('create', help='Create')
    parser_create.set_defaults(func=create_SEs)
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    args.func()


if __name__ == '__main__':
    main()