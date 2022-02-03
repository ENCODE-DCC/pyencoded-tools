#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

import encode_utils.connection as euc

import requests
import urllib.parse
import sys
import time
from time import sleep
import json
import pprint
import subprocess
import os
from time import sleep

# and treatment duration & amount & duration_units & amount_units? just
# treaments - match whole list - but can't query that way easily
# figure out how to handle life_stages - if anything other than unknown
# present, use that (and query also unknown) - what if 2 non-unknown present?

CONN = euc.Connection('prod', dry_run=True)

keypair = (os.environ.get('DCC_API_KEY'), os.environ.get('DCC_SECRET_KEY'))

GET_HEADERS = {'accept': 'application/json'}
POST_HEADERS = {'accept': 'application/json',
                'Content-Type': 'application/json'}
SERVER = "https://www.encodeproject.org/"
#SERVER = "https://test.encodedcc.org/"

DEBUG_ON = False

def encoded_get(url, keypair=None, frame='object', return_response=False):
    url_obj = urllib.parse.urlsplit(url)
    new_url_list = list(url_obj)
    query = urllib.parse.parse_qs(url_obj.query)
    if 'format' not in query:
        new_url_list[3] += "&format=json"
    if 'frame' not in query:
        new_url_list[3] += "&frame=%s" % (frame)
    if 'limit' not in query:
        new_url_list[3] += "&limit=all"
    if new_url_list[3].startswith('&'):
        new_url_list[3] = new_url_list[3].replace('&', '', 1)
    get_url = urllib.parse.urlunsplit(new_url_list)
    max_retries = 10
    max_sleep = 10
    while max_retries:
        try:
            if keypair:
                response = requests.get(get_url,
                                        auth=keypair,
                                        headers=GET_HEADERS)
            else:
                response = requests.get(get_url, headers=GET_HEADERS)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError) as e:
            print >> sys.stderr, e
            sleep(max_sleep - max_retries)
            max_retries -= 1
            continue
        else:
            if return_response:
                return response
            else:
                return response.json()


##################################
exp_param = [
    ('type', 'Experiment'),
    ('status', 'released'),
    # ('status', 'submitted'),
    # ('status', 'in progress'),
    ('analyses.pipeline_award_rfas', 'ENCODE4'),
    ('internal_status!', 'pipeline error'),
    ('award.project', 'ENCODE'),
    ('field', 'accession'),
    ('field', 'assay_term_name'),
    ('field', 'target'),
    ('field', 'biosample_ontology'),
    ('field', 'analyses'),
    ('field', 'bio_replicate_count')
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
    ('field', 'replicates.library.biosample.applied_modifications'),
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
    ('field', 'replicates.library.biosample.applied_modifications'),
    ('field', 'replicates.library.biosample.subcellular_fraction_term_name')
]
##################################


def create_SEs():

    url = "https://www.encodeproject.org/search/?type=Experiment&analyses.pipeline_award_rfas=ENCODE4&assay_term_name=ATAC-seq&assay_term_name=ChIP-seq&assay_term_name=DNase-seq&field=accession&field=analyses&field=assay_term_name&field=bio_replicate_count&field=biosample_ontology&field=replicates.library.biosample.donor&field=replicates.library.biosample.donor.life_stage&field=replicates.library.biosample.subcellular_fraction_term_name&field=replicates.library.biosample.treatments&field=target&internal_status%21=pipeline+error&control_type!=*&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens&status=released&limit=all"    
    exs =  encoded_get(url, keypair, frame='object')['@graph']
    print (len(exs))

        
    files_url = "https://www.encodeproject.org/search/?type=File&output_type=fold+change+over+control&output_type=read-depth+normalized+signal&status=released&limit=all"
    fils =  encoded_get(files_url, keypair, frame='object')['@graph']
    print (len(fils))

    file_ids = {}
    for file_obj in fils:
        file_ids[file_obj['@id']] = file_obj
        
    files_2_url = "https://www.encodeproject.org/search/?type=File&field=biological_replicates&status=released&output_category!=raw+data&file_format!=fastq&limit=all"
    fils2 =  encoded_get(files_2_url, keypair, frame='object')['@graph']
    print (len(fils2))

    file2_ids = {}
    for file_obj in fils2:
        file2_ids[file_obj['@id']] = file_obj

    print ("finished")

    '''
    mouse_exp_param = (exp_param + chip_param + other_param + mouse_adds).copy()
    results = CONN.search(list(mouse_exp_param))
    mouse_sets = {}
    for obj in results:
        assay = obj['assay_term_name']
        accession = obj['accession']
        target = obj.get('target', {}).get('label', 'none')
        biosample = obj['biosample_ontology']['name']
        reps = obj['bio_replicate_count']
        one_bio_obj = obj['replicates'][0]['library']['biosample']
        donor = obj['replicates'][0]['library']['biosample']['donor']['accession']
        age = one_bio_obj.get('age_display', 'unknown')
        files = []
        if 'analyses' in obj:
            for analysis in obj['analyses']:
                if analysis['status'] == 'released' and 'pipeline_award_rfas' in analysis:
                    if len(analysis['pipeline_award_rfas']) == 1 and 'ENCODE4' in analysis['pipeline_award_rfas']:
                        for file in analysis['files']:
                            # file_obj = CONN.get(file)
                            file_obj = file_ids.get(file, None)
                            if file_obj['status'] == 'released':
                                if assay in ['ChIP-seq', 'ATAC-seq']:
                                    if file_obj['output_type'] == 'fold change over control' and file_obj['file_format'] == 'bigWig':
                                        if reps > 1:
                                            if len(file_obj['biological_replicates']) > 1:
                                                if 'href' in file_obj:
                                                    files.append(file_obj['href'])
                                        else:
                                            if 'href' in file_obj:
                                                files.append(file_obj['href'])
                                else:
                                    if file_obj['output_type'] == 'read-depth normalized signal' and file_obj['file_format'] == 'bigWig' and 'preferred_default' in file_obj:
                                        if 'href' in file_obj:
                                            files.append(file_obj['href'])
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
            if len(files) == 1:
                for file in files:
                    mouse_sets[combination][target].append(file)
        if assay in ['DNase-seq', 'ATAC-seq']:
            mouse_sets[combination].setdefault(assay, [])
            mouse_sets[combination][assay].append(accession)
            if len(files) == 1:
                for file in files:
                    mouse_sets[combination][assay].append(file)

    for combination in mouse_sets:
        if all (k in mouse_sets[combination] for k in ('H3K27me3', 'H3K36me3', 'H3K4me1', 'H3K4me3', 'H3K27ac', 'H3K9me3')):
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
    '''
    ##################################
    human_exp_param = (exp_param + chip_param + other_param + human_adds).copy()
    results = CONN.search(list(human_exp_param))
    human_sets = {}
    counter = 0
    for obj in exs:
        counter += 1
        assay = obj['assay_term_name']
        accession = obj['accession']
        target = obj.get('target', {}).get('label', 'none')
        if target in ['H3K27me3', 'H3K36me3', 'H3K4me1', 'H3K4me3', 'H3K27ac', 'H3K9me3', 'CTCF', 'POLR2A', 'EP300', 'none']:
            biosample = obj['biosample_ontology']['name']
            reps = obj['bio_replicate_count']           
            one_bio_obj = obj['replicates'][0]['library']['biosample']
            donor = obj['replicates'][0]['library']['biosample']['donor']['accession']

            GM_accessions = []
            genetic_modifications = obj['replicates'][0]['library']['biosample'].get('applied_modifications', [])
            if 'applied_modifications' in obj['replicates'][0]['library']['biosample']:
                print('y')
            for GM in genetic_modifications:
                if GM['purpose'] != 'tagging':
                    GM_accessions.append(GM['accession'])
            GM_accessions = ','.join(sorted(GM_accessions))
            if GM_accessions == '':
                GM_accessions = 'no gm'


            files = set()
            if 'analyses' in obj:
                for analysis in obj['analyses']:
                    if analysis['status'] == 'released' and 'pipeline_award_rfas' in analysis:
                        if len(analysis['pipeline_award_rfas']) == 1 and 'ENCODE4' in analysis['pipeline_award_rfas']:
                            rep_list = set()
                            for file in analysis['files']:
                                file_obj = file2_ids.get(file, None)
                                if file_obj:
                                    for r in file_obj.get('biological_replicates', []):
                                        rep_list.add(r)

                            for file in analysis['files']:
                                file_obj = file_ids.get(file, None)
                                if file_obj and file_obj['status'] == 'released':
                                    if assay in ['ChIP-seq', 'ATAC-seq']:
                                        if file_obj['output_type'] == 'fold change over control' and file_obj['file_format'] == 'bigWig':
                                            if len(rep_list) > 1:
                                                if len(file_obj['biological_replicates']) > 1:
                                                    if 'href' in file_obj:
                                                        files.add(file_obj['href'])
                                                        break
                                            else:
                                                if 'href' in file_obj:
                                                    files.add(file_obj['href'])
                                                    break
                                    else:
                                        if file_obj['output_type'] == 'read-depth normalized signal' and file_obj['file_format'] == 'bigWig' and 'preferred_default' in file_obj:
                                            if 'href' in file_obj:
                                                files.add(file_obj['href'])
                                                break
            if files == set():
                print (files)
                print ("==========================>. A PROBLEM")
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

            combination = (biosample, life_stage, treatment, fraction, GM_accessions, donor)
            human_sets.setdefault(combination, {})
            if target in ['H3K27me3', 'H3K36me3', 'H3K4me1', 'H3K4me3', 'H3K27ac', 'H3K9me3', 'CTCF', 'POLR2A', 'EP300']:
                human_sets[combination].setdefault(target, [])
                human_sets[combination][target].append(accession)
                if len(files) == 1:
                    for file in files:
                        human_sets[combination][target].append(file)
            if assay in ['DNase-seq', 'ATAC-seq']:
                human_sets[combination].setdefault(assay, [])
                human_sets[combination][assay].append(accession)
                if len(files) == 1:
                    for file in files:
                        human_sets[combination][assay].append(file)

    f = open('/Users/jennifer/wrn30_output.txt','w')
    for combination in human_sets.keys():
        if all (k in human_sets[combination] for k in ('H3K27me3', 'H3K36me3', 'H3K4me1', 'H3K4me3', 'H3K27ac', 'H3K9me3')):
            url = "https://www.encodeproject.org/biosample-types/" + combination[0] + "/"
            btype_obj = encoded_get(url, keypair, frame='object')


            f.write('{}\t{}\t{}\t{}\t{}\n'.format(
                ' '.join([
                    str(combination[1]),
                    btype_obj['term_name'],
                    btype_obj['classification'],
                    combination[2],
                    'treated',
                    combination[3],
                    'fraction',
                    combination[4],
                    'gm',
                    combination[5],
                    'donor'
                ]),
                f'{combination[5]}_{combination[0]}',
                f'{combination[5]}_{combination[0]}_{combination[2]}_{combination[3]}_{combination[4]}',
                combination[0],
                human_sets[combination]
                ))
            

            print('{}\t{}\t{}\t{}\t{}\t'.format(
                ' '.join([
                    str(combination[1]),
                    btype_obj['term_name'],
                    btype_obj['classification'],
                    combination[2],
                    'treated',
                    combination[3],
                    'fraction',
                    combination[4],
                    'gm',
                    combination[5],
                    'donor'
                ]),
                f'{combination[5]}_{combination[0]}',
                f'{combination[5]}_{combination[0]}_{combination[2]}_{combination[3]}_{combination[4]}',
                combination[0],
                human_sets[combination]
                ))
            # print(
            #     ' '.join([
            #         str(combination[1]),
            #         btype_obj['term_name'],
            #         btype_obj['classification'],
            #         combination[2],
            #         'treated',
            #         combination[3],
            #         'fraction',
            #         combination[4],
            #         'gm',
            #         combination[5],
            #         'donor'
            #     ])
            # )
            # print(combination[0])
            # print(human_sets[combination])
            print('-----------------------')
    f.close()


def get_parser():
    parser = argparse.ArgumentParser(
        description='Script to find experiments and files for Segway pipeline on '
        'the ENCODE portal'
    )
    subparsers = parser.add_subparsers()
    parser_create = subparsers.add_parser('create', help='Create')
    parser_create.set_defaults(func=create_SEs)
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    try:
        func = args.func()
    except AttributeError:
        parser.error("too few arguments")
        func(args)


if __name__ == '__main__':
    main()