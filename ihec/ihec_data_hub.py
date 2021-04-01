#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Print experiment.xml
# https://github.com/IHEC/ihec-metadata/blob/master/specs/Ihec_metadata_specification.md
# Ihec_metadata_specification.md:
#     Chromatin Accessibility, WGBS, MeDIP-Seq, MRE-Seq, ChIP-Seq, RNA-Seq
# I added ATAC-seq, RRBS following existing data format
# Missing: microRNA counts, transcription profiling by array assay

# IHEC also requires format forllowing
# https://www.ebi.ac.uk/ena/submit/read-xml-format-1-5, see SRA.experiment.xsd


import argparse
from datetime import date
import json
import logging
import sys

import requests

BASE_URL = 'https://encodeproject.org/{}'

PROJECT_PROPS = {
    'ENCODE': {
        "description": "ENCODE reference epigenome",
        "description_url": "https://www.encodeproject.org/search/?type=ReferenceEpigenome&award.project=ENCODE",  # noqa: E501
        "email": "encode-help@lists.stanford.edu",
        "name": "ENCODE reference epigenome",
        "publishing_group": "ENCODE",
    },
    'Roadmap': {
        "description": "NIH Roadmap reference epigenome",
        "description_url": "https://www.encodeproject.org/search/?type=ReferenceEpigenome&award.project=Roadmap",  # noqa: E501
        "email": "encode-help@lists.stanford.edu",
        "name": "NIH Roadmap reference epigenome",
        "publishing_group": "NIH Roadmap",
    },
}
ASSEMBLY_PROPS = {
    'hg38': {"assembly": "hg38", "taxon_id": 9606},
    'hg19': {"assembly": "hg19", "taxon_id": 9606},
    'mm10': {"assembly": "mm10", "taxon_id": 10090},
}
# David from IHEC Data Hub asked us to submit just one hub JSON
# per project per assembly
merged_hubs = {
    ('ENCODE', 'hg38'): {
        'hub_description': {
            "date": date.today().strftime('%Y-%m-%d'),
            **PROJECT_PROPS['ENCODE'],
            **ASSEMBLY_PROPS['hg38']
        },
        'samples': {},
        'datasets': {}
    },
    ('ENCODE', 'hg19'): {
        'hub_description': {
            "date": date.today().strftime('%Y-%m-%d'),
            **PROJECT_PROPS['ENCODE'],
            **ASSEMBLY_PROPS['hg19']
        },
        'samples': {},
        'datasets': {}
    },
    ('ENCODE', 'mm10'): {
        'hub_description': {
            "date": date.today().strftime('%Y-%m-%d'),
            **PROJECT_PROPS['ENCODE'],
            **ASSEMBLY_PROPS['mm10']
        },
        'samples': {},
        'datasets': {}
    },
    ('Roadmap', 'hg38'): {
        'hub_description': {
            "date": date.today().strftime('%Y-%m-%d'),
            **PROJECT_PROPS['Roadmap'],
            **ASSEMBLY_PROPS['hg38']
        },
        'samples': {},
        'datasets': {}
    },
    ('Roadmap', 'hg19'): {
        'hub_description': {
            "date": date.today().strftime('%Y-%m-%d'),
            **PROJECT_PROPS['Roadmap'],
            **ASSEMBLY_PROPS['hg19']
        },
        'samples': {},
        'datasets': {}
    },
}


def main():
    parser = argparse.ArgumentParser(
        description='Prepare IHEC data hub for ENCODE reference epigenomes'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--one',
        dest='accessions',
        nargs='+',
        help='One ENCODE reference epigenomes accession to process.'
    )
    group.add_argument(
        '--all',
        action='store_true',
        help='One ENCODE reference epigenomes accession to process.'
    )
    args = parser.parse_args()

    if not args.all:
        ref_epi_ids = [
            '/reference-epigenomes/{}/'.format(acc)
            for acc in set(args.accessions)
        ]
    else:
        ref_epi_ids = [
            exp['@id'] for exp in requests.get(
                BASE_URL.format(
                    '/search/'
                    '?type=ReferenceEpigenome'
                    '&status=released'
                    '&field=accession'
                    '&limit=all'
                    '&format=json'
                )
            ).json()['@graph']
        ]

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='[%(asctime)s] %(name)s:%(levelname)s: %(message)s',
    )
    total = len(ref_epi_ids)
    hub_url = BASE_URL.format('/batch_hub/{}/{}/ihec.json')

    for i in range(total):
        ref_epi_id = ref_epi_ids[i]
        logging.info(
            'Generate [{}/{}] hub for {}'.format(i+1, total, ref_epi_id)
        )
        ref_epi = requests.get(
            BASE_URL.format(ref_epi_id) + '?format=json'
        ).json()
        project = ref_epi['award']['project']
        exps = ',,'.join(
            'accession={}'.format(exp['accession'])
            for exp in ref_epi['related_datasets']
            if exp['status'] == 'released'
        )
        if all(org['name'] == 'human' for org in ref_epi['organism']):
            assemblies = ['hg19', 'hg38']
        elif all(org['name'] == 'mouse' for org in ref_epi['organism']):
            assemblies = ['mm10']
        else:
            logging.error('Neither human nor mouse reference epigenome!')
            break

        for assembly in assemblies:
            hub_key = (project, assembly)
            logging.info(
                'Get {} hub from: {}'.format(
                    assembly, hub_url.format(exps, assembly)
                )
            )
            hub = requests.get(hub_url.format(exps, assembly)).json()
            logging.info(
                'Checking and registering data hub for {} {} {}'.format(
                    ref_epi_id, project, assembly
                )
            )
            for dataset_key in hub['datasets']:
                if dataset_key in merged_hubs[hub_key]['datasets']:
                    logging.error(
                        'Dataset {} from reference epigenome {} is found in '
                        'more than one reference epigenomes.'.format(
                            dataset_key, ref_epi_id
                        )
                    )
                else:
                    merged_hubs[hub_key]['datasets'][dataset_key] = hub[
                        'datasets'
                    ][dataset_key]
            for sample_key in hub['samples']:
                if sample_key in merged_hubs[hub_key]['samples']:
                    logging.error(
                        'Sample {} from reference epigenome {} is found in '
                        'more than one reference epigenomes.'.format(
                            sample_key, ref_epi_id
                        )
                    )
                else:
                    merged_hubs[hub_key]['samples'][sample_key] = hub[
                        'samples'
                    ][sample_key]

    output_fname = '{}_IHEC_Data_Hub_{}.json'
    for project, assembly in merged_hubs:
        if not merged_hubs[(project, assembly)]['datasets']:
            continue
        logging.info(
            'Saving {}'.format(output_fname.format(project, assembly))
        )
        with open(output_fname.format(project, assembly), 'w') as fo:
            json.dump(merged_hubs[(project, assembly)], fo)


if __name__ == "__main__":
    # execute only if run as a script
    main()
