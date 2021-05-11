#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import csv
import requests
import datetime
from itertools import chain
from pkg_resources import parse_version
import datetime

AUTH = (os.environ.get("DCC_API_KEY"), os.environ.get("DCC_SECRET_KEY"))
BASE_URL = 'https://www.encodeproject.org/{}/?format=json'
ENCODE4_CHIP_PIPELINES = [
    '/pipelines/ENCPL367MAS/',
    '/pipelines/ENCPL481MLO/',
    '/pipelines/ENCPL612HIG/',
    '/pipelines/ENCPL809GEM/',
]

def convert_date_string(date_string):
    if ":" == date_string[-3]:
        date_string = date_string[:-3]+date_string[-2:]
    return date_string


def get_latest_analysis(analyses):

    # preprocessing
    if not analyses:
        return None


    award_rfa_order = list(reversed([
        "ENCODE4",
        "ENCODE3",
        "ENCODE2",
        "ENCODE2-Mouse",
        "ENCODE",
        "GGR",
        "ENCORE",
        "Roadmap",
        "modENCODE",
        "modERN",
        "community"
    ]))
    assembly_order = list(reversed( [
        "GRCh38",
        "GRCh38-minimal",
        "hg19",
        "GRCm39",
        "mm10",
        "mm10-minimal",
        "mm9",
        "dm6",
        "dm3",
        "ce11",
        "ce10",
        "J02459.1"] + ["mixed"]
    ))
    genome_annotation_order = list(reversed(
        [
        "V33",
        "V30",
        "V29",
        "V24",
        "V22",
        "V19",
        "V10",
        "V7",
        "V3c",
        "miRBase V21",
        "M26",
        "M21",
        "M14",
        "M7",
        "M4",
        "M3",
        "M2",
        "ENSEMBL V65",
        "WS245",
        "WS235",
        "None"] + ['mixed']
    ))
    analysis_ranking = {}

    for analysis in analyses:
        
        # True ranked higher than False.
        lab_rank = '/labs/encode-processing-pipeline/' in analysis.get('pipeline_labs', [])
        # Get max index or zero if field doesn't exist.
        pipeline_award_rfa_rank = max(chain(
            (award_rfa_order.index(rfa)
            for rfa in analysis.get('pipeline_award_rfa', [])), [0])
        )
        # Get max index or zero if field doesn't exist. 
        assembly_rank = 0
        if analysis.get('assembly', '') in assembly_order:
            assembly_rank = assembly_order.index(analysis.get('assembly', ''))
        genome_annotation_rank = 0
        if analysis.get('genome_annotation', '') in genome_annotation_order:
            genome_annotation_rank = genome_annotation_order.index(analysis.get('genome_annotation', ''))
        # We reverse sort order at the end so later version numbers will rank higher.
        pipeline_version_rank = parse_version(analysis.get('pipeline_version', ''))
        # We reverse sort order at the end so later dates rank higher.
        date_rank = datetime.datetime.strptime(
            convert_date_string(
                analysis['date_created']
            ),
            "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        # Store all the ranking numbers for an analysis in a tuple that can be sorted.
        analysis_ranking[
            (
                lab_rank,
                pipeline_award_rfa_rank,
                assembly_rank,
                genome_annotation_rank,
                pipeline_version_rank,
                date_rank,
            )
        ] = analysis['@id']
    
    if analysis_ranking:
        # We want highest version, date, etc. so reverse sort order. Access value from top item.
        return (sorted(analysis_ranking.items(), reverse=True)[0][1])


def check_encode4_chip_pipeline(exp_acc):
    experiment = requests.get(BASE_URL.format(exp_acc), auth=AUTH).json()
    # Check for target and determine if histone ChIP
    is_histone = []
    is_dbgap = False
    if experiment.get('target'):
        is_histone = 'histone' in experiment['target']['investigated_as']

    # Check for dpGaP
    if experiment.get('internal_tags'):
        if 'dbGaP' in experiment.get('internal_tags'):
            is_dbgap = True

    print('------------------------------')
    print(exp_acc)
    print('------------------------------')
    bad_reason = []
    archiveAnalyses = {}
    archiveAnalyses[exp_acc] = []
    serious_audits = {
        'ERROR': len(experiment['audit'].get('ERROR', [])),
        'NOT_COMPLIANT': len(experiment['audit'].get('NOT_COMPLIANT', [])),
    }
    if serious_audits['ERROR']:
        print('The EXPERIMENT has {} ERROR audits'.format(serious_audits['ERROR']))
    if serious_audits['NOT_COMPLIANT']:
        print('The EXPERIMENT has {} NOT_COMPLIANT audits'.format(
            serious_audits['NOT_COMPLIANT']
        ))
    print('Number of original files: {}'.format(
        len(experiment['original_files'])
    ))
    analysisObj = experiment.get('analyses', [])
    latest = get_latest_analysis(analysisObj).split('/')[2]

    latest_analysis_object =  requests.get(BASE_URL.format(latest), auth=AUTH).json()

    serious_analysis_audits = {
            'ERROR': len(latest_analysis_object['audit'].get('ERROR', [])),
            'NOT_COMPLIANT': len(latest_analysis_object['audit'].get('NOT_COMPLIANT', [])),
        }
    if serious_analysis_audits['ERROR']:
        print('The latest analysis {} has {} ERROR audits'.format(latest, serious_analysis_audits['ERROR']))
    if serious_analysis_audits['NOT_COMPLIANT']:
        print('The latest analysis {} has {} NOT_COMPLIANT audits'.format(latest,
            serious_analysis_audits['NOT_COMPLIANT']
        ))
    

    print('Number of analyses: {}'.format(len(analysisObj)))
    print('File count in analyses: {}'.format(list(
        len(analysis['files']) for analysis in analysisObj
    )))
    skipped_ENC4_analyses_count = 0
    skipped_analyses_count = 0
    preferred_default_file_format = []
    preferred_default_output_type = set()
    rep_count = len({
        rep['biological_replicate_number']
        for rep in experiment['replicates']
    })
    rep_pair_count = rep_count * (rep_count - 1) // 2
    file_output_map = {}
    expected_file_output_count = {
        # Pooled peak only available for replicated (rep_count > 1) experiment
        'fold change over control': rep_count + int(rep_count > 1),
        'signal p-value': rep_count + int(rep_count > 1),
    }

    if is_dbgap:
        expected_file_output_count['redacted alignments'] = rep_count
        expected_file_output_count['pseudoreplicated peaks'] = (
            rep_count + int(rep_count > 1)
        ) * 2

        # Replicated peak (true replicated peak) only available for
        # replicated (rep_count > 1) experiment
        if rep_count > 1:
            expected_file_output_count['replicated peaks'] = rep_pair_count * 2
        expected_preferred_default_file_format = ['bigWig', 'bed', 'bigBed']
        expected_preferred_default_output_type = [
            'signal p-value', 'replicated peaks', 'pseudoreplicated peaks'
        ]

    elif is_histone:
        expected_file_output_count['unfiltered alignments'] = rep_count
        expected_file_output_count['alignments'] = rep_count
        expected_file_output_count['pseudoreplicated peaks'] = (
            rep_count + int(rep_count > 1)
        ) * 2
        # Replicated peak (true replicated peak) only available for
        # replicated (rep_count > 1) experiment
        if rep_count > 1:
            expected_file_output_count['replicated peaks'] = rep_pair_count * 2
        expected_preferred_default_file_format = ['bigWig', 'bed', 'bigBed']
        expected_preferred_default_output_type = [
            'signal p-value', 'replicated peaks', 'pseudoreplicated peaks'
        ]

    else:
        expected_file_output_count['unfiltered alignments'] = rep_count
        expected_file_output_count['alignments'] = rep_count
        expected_file_output_count.update(
            {
                'IDR ranked peaks':
                    rep_count + rep_pair_count + int(rep_count > 1),
                'IDR thresholded peaks': (rep_count + rep_pair_count) * 2,
            }
        )
        expected_preferred_default_file_format = ['bigWig', 'bed', 'bigBed']
        expected_preferred_default_output_type = [
            'signal p-value', 'IDR thresholded peaks', 'conservative IDR thresholded peaks'
        ]
        # Conservative peak (true replicated peak) only available for
        # replicated (rep_count > 1) experiment
        if rep_count > 1:
            expected_file_output_count[
                'conservative IDR thresholded peaks'
            ] = 2
    # Fix expectation for control experiments
    if experiment.get('control_type'): 
        if is_dbgap:
            expected_file_output_count = {
            'redacted alignments': rep_count
        }

        else:
            expected_file_output_count = {
            'unfiltered alignments': rep_count,
            'alignments': rep_count
            }

    for analysis in analysisObj:

        # archive all other released analyses
        if analysis['status'] in ['released'] and analysis['accession'] != latest:
            archiveAnalyses[exp_acc].append(analysis['accession'])

        analysisStatus = ["released", "in progress", "archived"]
        if sorted(analysis['pipelines']) != ENCODE4_CHIP_PIPELINES:
            skipped_analyses_count += 1
            continue

        if sorted(analysis['pipelines']) == ENCODE4_CHIP_PIPELINES and analysis['accession'] != latest:
            skipped_ENC4_analyses_count += 1
            continue 

        print('Analysis object {} being checked'.format(analysis['accession']))

        if analysis.get('assembly') not in ['GRCh38', 'mm10']:
            print('Wrong assembly')
            bad_reason.append('Wrong assembly')
        if analysis.get('genome_annotation'):
            print('Has genome annotation')
            bad_reason.append('Has genome annotation')
        for fid in analysis['files']:
            f_obj = requests.get(BASE_URL.format(fid), auth=AUTH).json()
            file_output_map.setdefault(f_obj['output_type'], 0)
            file_output_map[f_obj['output_type']] += 1
            if f_obj.get('preferred_default'):
                preferred_default_file_format.append(f_obj['file_format'])
                preferred_default_output_type.add(f_obj['output_type'])
        # Different expectation for control experiments
        if experiment.get('control_type'):
            if (
                len(preferred_default_file_format) != 0
                or len(preferred_default_output_type) != 0
            ):
                print('Control experiment has preferred default by mistake')
                bad_reason.append(
                    'Control experiment has preferred default by mistake'
                )
        else:
            if sorted(
                preferred_default_file_format
            ) != sorted(expected_preferred_default_file_format):
                msg = 'Wrong preferred default file format'
                if rep_count == 1:
                    msg += '; unreplicated experiment'
                print(msg)
                bad_reason.append(msg)
            if (
                len(preferred_default_output_type) != 2
                or list(
                    preferred_default_output_type
                )[0] not in expected_preferred_default_output_type
                or list(
                    preferred_default_output_type
                )[1] not in expected_preferred_default_output_type
            ):
                msg = 'Wrong preferred default file output type'
                if rep_count == 1:
                    msg += '; unreplicated experiment'
                print(msg)
                bad_reason.append(msg)
        if file_output_map != expected_file_output_count:
            print('Wrong file output type map')
            bad_reason.append('Wrong file output type map')
            print('Has {}'.format(str(file_output_map)))
            print('Expect {}'.format(str(expected_file_output_count)))


    if skipped_ENC4_analyses_count > 0:
        print('Skipped {} old ENCODE4 uniform analyses'.format(
            skipped_ENC4_analyses_count
        ))
    if skipped_analyses_count == len(analysisObj):
        print('No ENCODE4 analysis found')
        bad_reason.append('No ENCODE4 analysis found')
    if skipped_analyses_count:
        print('Skipped {} non-ENCODE4 uniform analyses'.format(
            skipped_analyses_count
        ))
    print('')
    return bad_reason, serious_analysis_audits, archiveAnalyses


def get_parser():
    parser = argparse.ArgumentParser(
        description='Script to check result of ENCODE4 ChIP-seq processing on '
        'the ENCODE portal'
    )
    parser.add_argument(
        'exp_accs',
        nargs='*',
        default=sys.stdin,
        help='One or more experiment accessions (ENCSRs).'
    )
    parser.add_argument(
        '--ticket', dest='ticket',
        help='related ticket number (PROD###)'
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    summary = {}
    GoodExperiments = {}
    patchAnalyses = {}
    for exp_acc in args.exp_accs:
        bad_reason, serious_audits, archiveAnalyses = check_encode4_chip_pipeline(
            exp_acc.strip()
        )
        status = ', '.join(bad_reason) or 'Good'
        if status == 'Good':
            experimentID = exp_acc.strip()
            GoodExperiments[experimentID] = sum(serious_audits.values())
        if sum(serious_audits.values()):
            status += ' BUT has {} ERROR and {} NOT_COMPLIANT'.format(
                serious_audits.get('ERROR', 0),
                serious_audits.get('NOT_COMPLIANT', 0),
            )

        summary[exp_acc.strip()] = status
        patchAnalyses[exp_acc.strip()] = archiveAnalyses[exp_acc.strip()]

    if args.ticket:
        analysisArchive_filename = '%s_analysisStatusPatch.txt' % (args.ticket).strip()
        release_filename = '%s_releasedPatch.txt' % (args.ticket).strip()
        problem_filename = '%s_internalStatusPatch.txt' % (args.ticket).strip()
        
    else:
        analysisArchive_filename = 'analysisStatusPatch.txt'
        release_filename = 'releasedPatch.txt'
        problem_filename = 'internalStatusPatch.txt'

    releasedFiles = open(release_filename, 'w+')
    problemFiles = open(problem_filename, 'w+')
    analysisPatch = open(analysisArchive_filename, 'w+')
    analysisWriter = csv.writer(analysisPatch, delimiter='\t')
    analysisWriter.writerow(['record_id', 'status'])
    problemWriter = csv.writer(problemFiles, delimiter='\t')
    problemWriter.writerow(['record_id', 'internal_status'])   

    for exp_acc in summary:
        print('{}: {}'.format(exp_acc, summary[exp_acc]))
        if patchAnalyses[exp_acc]:
            print('Older released analyses for {} found: {}'.format(exp_acc, patchAnalyses[exp_acc]))
        print('')
        
        try:
            for analysis in patchAnalyses[exp_acc.strip()]:
                analysisWriter.writerow([analysis, 'archived'])
        except KeyError:
            continue

    if GoodExperiments:
        for key in GoodExperiments:
            if GoodExperiments[key]:
                problemWriter.writerow([key, 'post-pipeline review'])
            else:
                releasedFiles.write(key)
                releasedFiles.write('\n')
                problemWriter.writerow([key, 'release ready'])


if __name__ == '__main__':
    main()
