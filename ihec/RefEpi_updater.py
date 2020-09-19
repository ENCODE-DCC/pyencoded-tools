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
    ('field', 'accession')
]

core_marks_param = [
    ('assay_term_name', 'ChIP-seq'),
    ('target.label', 'H3K27me3'),
    ('target.label', 'H3K36me3'),
    ('target.label', 'H3K4me1'),
    ('target.label', 'H3K4me3'),
    ('target.label', 'H3K27ac'),
    ('target.label', 'H3K9me3'),
    ('field', 'target.label'),
    ('field', 'biosample_ontology')
]

ref_epi_control_adds = [
    ('field', 'related_datasets.accession'),
    ('field', 'related_datasets.control_type'),
    ('field', 'related_datasets.possible_controls.@id'),
    ('field', 'related_datasets.possible_controls.accession'),
]

mouse_adds = [
    (
        'replicates.library.biosample.donor.organism.scientific_name',
        'Mus musculus'
    ),
    ('field', 'replicates.library.biosample.age_display'),
    ('field', 'replicates.library.biosample.treatments'),
    ('field', 'replicates.library.biosample.subcellular_fraction_term_name')
]

human_adds = [
    (
        'replicates.library.biosample.donor.organism.scientific_name',
        'Homo sapiens'
    ),
    ('replicates.library.biosample.donor.internal_tags!', 'ENTEx'),
    ('field', 'replicates.library.biosample.treatments'),
    ('field', 'replicates.library.biosample.donor.life_stage'),
    ('field', 'replicates.library.biosample.subcellular_fraction_term_name')
]

entex_adds = [
    ('replicates.library.biosample.donor.internal_tags', 'ENTEx'),
    ('field', 'replicates.library.biosample.donor.accession')
]

base_refepi_param = [
    ('type', 'ReferenceEpigenome'),
    ('status', 'released'),
    ('status', 'in progress'),
    ('award.project', 'ENCODE'),
    ('field', 'biosample_ontology'),
    ('field', 'accession')
]

mouse_refepi_adds = [
    ('organism.scientific_name', 'Mus musculus'),
    ('field', 'related_datasets.replicates.library.biosample')
]

human_refepi_adds = [
    ('organism.scientific_name', 'Homo sapiens'),
    (
        'related_datasets.replicates.library.biosample.donor.internal_tags!',
        'ENTEx'
    ),
    (
        'field',
        'related_datasets.replicates.library.biosample.donor.life_stage'
    ),
    ('field', 'related_datasets.replicates.library.biosample.treatments'),
    (
        'field',
        'related_datasets.replicates.library.biosample.'
        'subcellular_fraction_term_name'
    )
]

entex_refepi_adds = [
    (
        'related_datasets.replicates.library.biosample.donor.internal_tags',
        'ENTEx'
    ),
    ('field', 'related_datasets.replicates.library.biosample.donor.accession')
]
##################################


def is_subset(query_combination, target_combination):
    if len(query_combination) != len(target_combination):
        return False
    for i in range(len(query_combination)):
        if (
            isinstance(query_combination[i], (list, tuple, set))
            and isinstance(target_combination[i], (list, tuple, set))
        ):
            if not set(query_combination[i]) <= set(target_combination[i]):
                return False
        elif query_combination[i] != target_combination[i]:
            return False
    return True


def make_matrix(refepi_queries, assay_queries):
    with open('refepi_matrix.txt', 'w') as fout:
        fout.write('\t'.join([''] + list(assay_queries.keys())) + '\n')
        total = len(refepi_queries)
        for i, refepi in enumerate(refepi_queries):
            print('---------------------------------------------------')
            print('Checking {}/{} reference epigenome'.format(i, total))
            print('---------------------------------------------------')
            current_assays = ['{}_current'.format(refepi)]
            candidate_assays = ['{}_candidate'.format(refepi)]
            for assay in assay_queries:
                current_datasets = CONN.search(
                    # Will lose archived datasets currently in a reference
                    # epigenome if using refepi_queries[refepi]
                    [
                        ('type', 'Experiment'),
                        ('field', 'accession'),
                        ('related_series.accession', refepi),
                    ]
                    + assay_queries[assay]
                )
                if len(current_datasets) > 1:
                    print(
                        '{} has unexpectedly {} {} dataset'.format(
                            refepi, len(current_datasets), assay
                        )
                    )
                if len(current_datasets) == 1:
                    current_assays.append(
                        '=HYPERLINK("{}/{}","{}")'.format(
                            CONN.dcc_url,
                            current_datasets[0]['accession'],
                            current_datasets[0]['accession'],
                        )
                    )
                else:
                    non_field_params = []
                    for param in refepi_queries[refepi]:
                        if param[0] != 'field':
                            non_field_params.append(param)
                    non_json_search_url = CONN.make_search_url(
                        non_field_params
                        + assay_queries[assay]
                        + [('related_series.accession!', refepi)]
                    )
                    current_assays.append(
                        '=HYPERLINK("{}",{})'.format(
                            non_json_search_url, repr(len(current_datasets))
                        )
                    )
                candidate_datasets = CONN.search(
                    refepi_queries[refepi]
                    + [('related_series.accession!', refepi)]
                    + assay_queries[assay]
                )
                if len(candidate_datasets) == 1:
                    candidate_assays.append(
                        '=HYPERLINK("{}/{}","{}")'.format(
                            CONN.dcc_url,
                            candidate_datasets[0]['accession'],
                            candidate_datasets[0]['accession'],
                        )
                    )
                else:
                    non_field_params = []
                    for param in refepi_queries[refepi]:
                        if param[0] != 'field':
                            non_field_params.append(param)
                    non_json_search_url = CONN.make_search_url(
                        non_field_params
                        + [('related_series.accession!', refepi)]
                        + assay_queries[assay]
                    )
                    candidate_assays.append(
                        '=HYPERLINK("{}",{})'.format(
                            non_json_search_url, repr(len(candidate_datasets))
                        )
                    )
            fout.write('\t'.join(current_assays) + '\n')
            fout.write('\t'.join(candidate_assays) + '\n')


def grab_ref_epis():
    mouse_refepis = {}
    mouse_refepi_param = base_refepi_param.copy() + mouse_refepi_adds.copy()
    results = CONN.search(list(mouse_refepi_param))
    for obj in results:
        accession = obj['accession']
        biosample = obj['biosample_ontology'][0]['name']
        one_bio_obj = obj['related_datasets'][0]['replicates'][0]['library'][
            'biosample'
        ]
        age = one_bio_obj.get('age_display', 'unknown')
        treatment = 'no treatment'
        if len(one_bio_obj['treatments']) > 0:
            treatment = one_bio_obj['treatments'][0]['treatment_term_name']
        fraction = one_bio_obj.get(
            'subcellular_fraction_term_name', 'no fraction'
        )
        mouse_refepis[accession] = (biosample, age, treatment, fraction)

    human_refepis = {}
    human_refepi_param = base_refepi_param.copy() + human_refepi_adds.copy()
    results = CONN.search(list(human_refepi_param))
    for obj in results:
        accession = obj['accession']
        biosample = obj['biosample_ontology'][0]['name']
        one_bio_obj = obj['related_datasets'][0]['replicates'][0]['library'][
            'biosample'
        ]

        life_stage = tuple(
            sorted(
                set(
                    replicate['library']['biosample'].get('donor', {}).get(
                        'life_stage', 'unknown'
                    )
                    for related_dataset in obj['related_datasets']
                    for replicate in related_dataset['replicates']
                )
            )
        )

        treatment = 'no treatment'
        if len(one_bio_obj['treatments']) > 0:
            treatment = one_bio_obj['treatments'][0]['treatment_term_name']
        fraction = one_bio_obj.get(
            'subcellular_fraction_term_name', 'no fraction'
        )
        human_refepis[accession] = (biosample, life_stage, treatment, fraction)

    entex_refepis = {}
    entex_refepi_param = base_refepi_param.copy() + entex_refepi_adds.copy()
    results = CONN.search(list(entex_refepi_param))
    for obj in results:
        accession = obj['accession']
        biosample = obj['biosample_ontology'][0]['name']
        donor = obj['related_datasets'][0]['replicates'][0]['library'][
            'biosample'
        ]['donor']['accession']
        entex_refepis[accession] = (biosample, donor)

    current_refepis = {
        'mouse': mouse_refepis,
        'human': human_refepis,
        'entex': entex_refepis
        }

    return current_refepis


def create_REs():
    current_refepis = grab_ref_epis()

    mouse_exp_param = (exp_param + core_marks_param + mouse_adds).copy()
    results = CONN.search(list(mouse_exp_param))
    mouse_sets = {}
    for obj in results:
        accession = obj['accession']
        target = obj['target']['label']
        biosample = obj['biosample_ontology']['name']
        one_bio_obj = obj['replicates'][0]['library']['biosample']
        age = one_bio_obj.get('age_display', 'unknown')
        treatment = 'no treatment'
        if len(one_bio_obj['treatments']) > 0:
            treatment = one_bio_obj['treatments'][0]['treatment_term_name']
        fraction = one_bio_obj.get(
            'subcellular_fraction_term_name', 'no fraction'
        )
        combination = (biosample, age, treatment, fraction)
        mouse_sets.setdefault(combination, {})
        mouse_sets[combination].setdefault(target, [])
        mouse_sets[combination][target].append(accession)

    for combination in mouse_sets:
        if (
            len(mouse_sets[combination].keys()) > 2
            and not any(
                is_subset(combination, curr_comb)
                for curr_comb in current_refepis['mouse'].values()
            )
        ):
            btype_obj = CONN.get('/biosample-types/{}/'.format(combination[0]))
            print(
                ' '.join([
                    str(combination[1]),
                    btype_obj['term_name'],
                    btype_obj['classification'],
                    combination[2],
                    'treated',
                    combination[3],
                    'fraction'
                ])
            )
            print(combination[0])
            print(mouse_sets[combination])
            print('-----------------------')
    ##################################
    human_exp_param = (exp_param + core_marks_param + human_adds).copy()
    results = CONN.search(list(human_exp_param))
    human_sets = {}
    for obj in results:
        accession = obj['accession']
        target = obj['target']['label']
        biosample = obj['biosample_ontology']['name']
        one_bio_obj = obj['replicates'][0]['library']['biosample']

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

        combination = (biosample, life_stage, treatment, fraction)
        human_sets.setdefault(combination, {})
        human_sets[combination].setdefault(target, [])
        human_sets[combination][target].append(accession)

    for combination in human_sets.keys():
        if (
            len(human_sets[combination].keys()) > 2
            and not any(
                is_subset(combination, curr_comb)
                for curr_comb in current_refepis['human'].values()
            )
        ):
            btype_obj = CONN.get('/biosample-types/{}/'.format(combination[0]))
            print(
                ' '.join([
                    str(combination[1]),
                    btype_obj['term_name'],
                    btype_obj['classification'],
                    combination[2],
                    'treated',
                    combination[3],
                    'fraction'
                ])
            )
            print(combination[0])
            print(human_sets[combination])
            print('-----------------------')
    ##################################
    entex_exp_param = (exp_param + core_marks_param + entex_adds).copy()
    results = CONN.search(list(entex_exp_param))
    entex_sets = {}
    for obj in results:
        accession = obj['accession']
        target = obj['target']['label']
        biosample = obj['biosample_ontology']['name']
        donor = obj['replicates'][0]['library']['biosample']['donor'][
            'accession'
        ]

        combination = (biosample, donor)

        entex_sets.setdefault(combination, {})
        entex_sets[combination].setdefault(target, [])
        entex_sets[combination][target].append(accession)

    for combination in entex_sets.keys():
        if (
            len(entex_sets[combination].keys()) > 2
            and not any(
                is_subset(combination, curr_comb)
                for curr_comb in current_refepis['entex'].values()
            )
        ):
            btype_obj = CONN.get('/biosample-types/{}/'.format(combination[0]))
            print(' '.join([btype_obj['term_name'], str(combination[1])]))
            print(entex_sets[combination])
            print('-----------------------')


def update_REs():
    current_refepis = grab_ref_epis()

    assay_queries = {
        'H3K27me3': [
            ('assay_term_name', 'ChIP-seq'), ('target.label', 'H3K27me3')
        ],
        'H3K36me3': [
            ('assay_term_name', 'ChIP-seq'), ('target.label', 'H3K36me3')
        ],
        'H3K4me1': [
            ('assay_term_name', 'ChIP-seq'), ('target.label', 'H3K4me1')
        ],
        'H3K4me3': [
            ('assay_term_name', 'ChIP-seq'), ('target.label', 'H3K4me3')
        ],
        'H3K27ac': [
            ('assay_term_name', 'ChIP-seq'), ('target.label', 'H3K27ac')
        ],
        'H3K9me3': [
            ('assay_term_name', 'ChIP-seq'), ('target.label', 'H3K9me3')
        ],
        'CTCF': [('assay_term_name', 'ChIP-seq'), ('target.label', 'CTCF')],
        'POLR2A': [
            ('assay_term_name', 'ChIP-seq'),
            ('target.label', 'POLR2A')
        ],
        'POLR2AphosphoS5': [
            ('assay_term_name', 'ChIP-seq'),
            ('target.label', 'POLR2AphosphoS5')
        ],
        'EP300': [('assay_term_name', 'ChIP-seq'), ('target.label', 'EP300')],
        'WGBS': [('assay_title', 'WGBS')],
        'total RNA-seq': [('assay_title', 'total RNA-seq')],
        'polyA RNA-seq': [('assay_title', 'polyA plus RNA-seq')],
        'miRNA-seq': [('assay_title', 'microRNA-seq')],
        'smRNA-seq': [('assay_title', 'small RNA-seq')],
        'DNase-seq': [('assay_title', 'DNase-seq')],
        'ATAC-seq': [('assay_title', 'ATAC-seq')]
        }

    refepi_queries = {}
    ##################################
    for refepi in current_refepis['mouse']:
        biosample, age, treatment, fraction = current_refepis['mouse'][refepi]
        mouse_specifics = [
            ('biosample_ontology.name', biosample),
            ('replicates.library.biosample.age_display', age)
        ]
        if treatment == 'no treatment':
            mouse_specifics.append(
                (
                    'replicates.library.biosample.treatments'
                    '.treatment_term_name!',
                    '*'
                )
            )
        else:
            mouse_specifics.append(
                (
                    'replicates.library.biosample.treatments'
                    '.treatment_term_name',
                    treatment
                )
            )
        if fraction == 'no fraction':
            mouse_specifics.append(
                (
                    'replicates.library.biosample'
                    '.subcellular_fraction_term_name!',
                    '*'
                )
            )
        else:
            mouse_specifics.append(
                (
                    'replicates.library.biosample'
                    '.subcellular_fraction_term_name',
                    fraction
                )
            )
        refepi_queries[refepi] = exp_param + mouse_adds + mouse_specifics
    ##################################
    for refepi in current_refepis['human'].keys():
        biosample, life_stage, treatment, fraction = current_refepis['human'][
            refepi
        ]
        human_specifics = [('biosample_ontology.name', biosample)]
        human_specifics.extend(
            ('replicates.library.biosample.donor.life_stage', stage)
            for stage in life_stage
        )
        if treatment == 'no treatment':
            human_specifics.append(
                (
                    'replicates.library.biosample.treatments'
                    '.treatment_term_name!',
                    '*'
                )
            )
        else:
            human_specifics.append(
                (
                    'replicates.library.biosample.treatments'
                    '.treatment_term_name',
                    treatment
                )
            )
        if fraction == 'no fraction':
            human_specifics.append(
                (
                    'replicates.library.biosample'
                    '.subcellular_fraction_term_name!',
                    '*'
                )
            )
        else:
            human_specifics.append(
                (
                    'replicates.library.biosample'
                    '.subcellular_fraction_term_name',
                    fraction
                )
            )
        refepi_queries[refepi] = exp_param + human_adds + human_specifics
    ##################################
    for refepi in current_refepis['entex'].keys():
        biosample, donor = current_refepis['entex'][refepi]
        entex_specifics = [
            ('biosample_ontology.name', biosample),
            ('replicates.library.biosample.donor.accession', donor)
        ]
        refepi_queries[refepi] = exp_param + entex_adds + entex_specifics
    ##################################
    make_matrix(refepi_queries, assay_queries)


def find_controls():
    # check 2: any ChIP ctrls in REs without controlling 1+ in the same RE?
    refepi_param_for_controls = (
        base_refepi_param + ref_epi_control_adds
    ).copy()
    results = CONN.search(list(refepi_param_for_controls))
    with open('control_patches.tsv', 'w') as f:
        f.write('record_id\trelated_datasets\n')
        for obj in results:
            non_control_experiments = set()
            current_controls = set()
            expected_controls = set()
            for exp in obj['related_datasets']:
                if exp.get('control_type'):
                    current_controls.add(exp['accession'])
                else:
                    non_control_experiments.add(exp['accession'])
                    expected_controls |= {
                        control['accession']
                        for control in exp.get('possible_controls', [])
                        if control['@id'].startswith('/experiments/ENCSR')
                    }
            if current_controls == expected_controls:
                continue
            f.write(
                '{}\t{}\n'.format(
                    obj['accession'],
                    ','.join(
                        sorted(non_control_experiments | expected_controls)
                    ),
                )
            )


def get_parser():
    parser = argparse.ArgumentParser(
        description='Script to maintain ReferenceEpigenome on '
        'the ENCODE portal'
    )
    subparsers = parser.add_subparsers()
    parser_create = subparsers.add_parser('create', help='Create')
    parser_create.set_defaults(func=create_REs)
    parser_update = subparsers.add_parser('update', help='Update')
    parser_update.set_defaults(func=update_REs)
    parser_find_control = subparsers.add_parser(
        'find-controls', help='Find control'
    )
    parser_find_control.set_defaults(func=find_controls)
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    args.func()


if __name__ == '__main__':
    main()
