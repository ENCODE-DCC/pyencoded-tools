#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Print experiment.xml
# https://github.com/IHEC/ihec-metadata/blob/master/specs/Ihec_metadata_specification.md
# Ihec_metadata_specification.md:
#     Chromatin Accessibility, WGBS, MeDIP-Seq, MRE-Seq, ChIP-Seq, RNA-Seq
# I added ATAC-seq, RRBS following existing data format
# Missing: microRNA counts, transcription profiling by array assay

# IHEC aslo requires format forllowing
# https://www.ebi.ac.uk/ena/submit/read-xml-format-1-5, see SRA.experiment.xsd


import argparse
from datetime import date
import json
import logging
import sys

import requests


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

    base_url = 'https://encd-2401-ihec-hub-yunhailuo.demo.encodedcc.org/{}'
    if not args.all:
        ref_epi_ids = [
            '/reference-epigenomes/{}/'.format(acc)
            for acc in set(args.accessions)
        ]
    else:
        ref_epi_ids = [
            exp['@id'] for exp in requests.get(
                base_url.format(
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
    hub_url = base_url.format('/batch_hub/{}/{}/ihec.json')
    # David from IHEC Data Hub asked us to submit just one hub JSON per
    # assembly
    merged_hubs = {
        'hg19': {
            'hub_description': {
                "assembly": "hg19",
                "date": date.today().strftime('%Y-%m-%d'),
                "description": "ENCODE reference epigenome",
                "description_url": "https://www.encodeproject.org/search/?type=ReferenceEpigenome",  # noqa: E501
                "email": "encode-help@lists.stanford.edu",
                "name": "ENCODE reference epigenome",
                "publishing_group": "ENCODE",
                "taxon_id": 9606
            },
            'samples': {},
            'datasets': {}
        },
        'hg38': {
            'hub_description': {
                "assembly": "hg38",
                "date": date.today().strftime('%Y-%m-%d'),
                "description": "ENCODE reference epigenome",
                "description_url": "https://www.encodeproject.org/search/?type=ReferenceEpigenome",  # noqa: E501
                "email": "encode-help@lists.stanford.edu",
                "name": "ENCODE reference epigenome",
                "publishing_group": "ENCODE",
                "taxon_id": 9606
            },
            'samples': {},
            'datasets': {}
        },
        'mm10': {
            'hub_description': {
                "assembly": "mm10",
                "date": date.today().strftime('%Y-%m-%d'),
                "description": "ENCODE reference epigenome",
                "description_url": "https://www.encodeproject.org/search/?type=ReferenceEpigenome",  # noqa: E501
                "email": "encode-help@lists.stanford.edu",
                "name": "ENCODE reference epigenome",
                "publishing_group": "ENCODE",
                "taxon_id": 10090
            },
            'samples': {},
            'datasets': {}
        }
    }

    for i in range(total):
        ref_epi_id = ref_epi_ids[i]
        logging.info(
            'Generate [{}/{}] hub for {}'.format(i+1, total, ref_epi_id)
        )
        ref_epi = requests.get(
            base_url.format(ref_epi_id) + '?format=json'
        ).json()
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
            logging.info(
                'Get {} hub from: {}'.format(
                    assembly, hub_url.format(exps, assembly)
                )
            )
            hub = requests.get(hub_url.format(exps, assembly)).json()
            logging.info(
                'Checking and registering data hub for {} {}'.format(
                    ref_epi_id, assembly
                )
            )
            for dataset_key in hub['datasets']:
                if dataset_key in merged_hubs[assembly]['datasets']:
                    logging.error(
                        'Dataset {} from reference epigenome {} is found in '
                        'more than one reference epigenomes.'.format(
                            dataset_key, ref_epi_id
                        )
                    )
                else:
                    merged_hubs[assembly]['datasets'][dataset_key] = hub[
                        'datasets'
                    ][dataset_key]
            for sample_key in hub['samples']:
                if sample_key in merged_hubs[assembly]['samples']:
                    logging.error(
                        'Sample {} from reference epigenome {} is found in '
                        'more than one reference epigenomes.'.format(
                            sample_key, ref_epi_id
                        )
                    )
                else:
                    merged_hubs[assembly]['samples'][sample_key] = hub[
                        'samples'
                    ][sample_key]

    output_fname = 'ENCODE_IHEC_Data_Hub_{}.json'
    for assembly in merged_hubs:
        if not merged_hubs[assembly]['datasets']:
            continue
        logging.info('Saving {}'.format(output_fname.format(assembly)))
        with open(output_fname.format(assembly), 'w') as fo:
            json.dump(merged_hubs[assembly], fo)


if __name__ == "__main__":
    # execute only if run as a script
    main()
