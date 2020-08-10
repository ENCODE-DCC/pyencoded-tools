#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import csv
import requests

AUTH = (os.environ.get("DCC_API_KEY"), os.environ.get("DCC_SECRET_KEY"))
BASE_URL = 'https://www.encodeproject.org/{}/?format=json'
ENCODE4_CHIP_PIPELINES = [
    '/pipelines/ENCPL367MAS/',
    '/pipelines/ENCPL481MLO/',
    '/pipelines/ENCPL612HIG/',
    '/pipelines/ENCPL809GEM/',
]
PREFERRED_DEFAULT_FILE_FORMAT = ['bed', 'bigBed']
PREFERRED_DEFAULT_OUTPUT_TYPE = [
    'IDR thresholded peaks', 'conservative IDR thresholded peaks'
]


def check_encode4_chip_pipeline(exp_acc):
    experiment = requests.get(BASE_URL.format(exp_acc), auth=AUTH).json()
    print('------------------------------')
    print(exp_acc)
    print('------------------------------')
    bad_reason = []
    serious_audits = {
        'ERROR': len(experiment['audit'].get('ERROR', [])),
        'NOT_COMPLIANT': len(experiment['audit'].get('NOT_COMPLIANT', [])),
    }
    if serious_audits['ERROR']:
        print('Has {} ERROR audits'.format(serious_audits['ERROR']))
    if serious_audits['NOT_COMPLIANT']:
        print('Has {} NOT_COMPLIANT audits'.format(
            serious_audits['NOT_COMPLIANT']
        ))
    print('Number of original files: {}'.format(
        len(experiment['original_files'])
    ))
    print('Number of analyses: {}'.format(len(experiment['analyses'])))
    print('File count in analyses: {}'.format(list(
        len(analsis['files']) for analsis in experiment['analyses']
    )))
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
        'unfiltered alignments': rep_count,
        'alignments': rep_count,
        # Pooled peak only available for replicated (rep_count > 1) experiment
        'fold change over control': rep_count + int(rep_count > 1),
        'signal p-value': rep_count + int(rep_count > 1),
        'IDR ranked peaks': rep_count + rep_pair_count + int(rep_count > 1),
        'IDR thresholded peaks': (rep_count + rep_pair_count) * 2,
    }
    # Conservative peak (true replicated peak) only available for
    # replicated (rep_count > 1) experiment
    if rep_count > 1:
        expected_file_output_count['conservative IDR thresholded peaks'] = 2
    # Fix expectation for control experiments
    if experiment.get('control_type'):
        expected_file_output_count = {
            'unfiltered alignments': rep_count,
            'alignments': rep_count,
        }
    for analysis in experiment['analyses']:
        if sorted(analysis['pipelines']) != ENCODE4_CHIP_PIPELINES:
            skipped_analyses_count += 1
            continue
        if analysis['assemblies'] != ['GRCh38']:
            print('Wrong assembly')
            bad_reason.append('Wrong assembly')
        if analysis['genome_annotations'] != []:
            print('Has genome annotations')
            bad_reason.append('Has genome annotations')
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
            ) != PREFERRED_DEFAULT_FILE_FORMAT:
                print('Wrong preferred default file format')
                bad_reason.append('Wrong preferred default file format')
            if (
                len(preferred_default_output_type) != 1
                or list(
                    preferred_default_output_type
                )[0] not in PREFERRED_DEFAULT_OUTPUT_TYPE
            ):
                print('Wrong preferred default file output type')
                bad_reason.append('Wrong preferred default file output type')
        if file_output_map != expected_file_output_count:
            print('Wrong file output type map')
            bad_reason.append('Wrong file output type map')
            print('Has {}'.format(str(file_output_map)))
            print('Expect {}'.format(str(expected_file_output_count)))
    if skipped_analyses_count == len(experiment['analyses']):
        print('No ENCODE4 analysis found')
        bad_reason.append('No ENCODE4 analysis found')
    elif skipped_analyses_count:
        print('Skipped {} non-ENCODE4 uniform analyses'.format(
            skipped_analyses_count
        ))
    print('')
    return bad_reason, serious_audits


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
    for exp_acc in args.exp_accs:
        bad_reason, serious_audits = check_encode4_chip_pipeline(
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

    for exp_acc in summary:
        print('{}: {}'.format(exp_acc, summary[exp_acc]))

    if GoodExperiments:
        if args.ticket:
            release_filename = '%s_releasedPatch.txt' % (args.ticket).strip()
            problem_filename = '%s_internalstatusPatch.txt' % (args.ticket).strip()
        else:
            release_filename = 'releasedPatch.txt'
            problem_filename = 'internalstatusPatch.txt'

        releasedFiles = open(release_filename, 'w+')
        problemFiles = open(problem_filename, 'w+')
        problemWriter = csv.writer(problemFiles, delimiter='\t')
        problemWriter.writerow(['record_id', 'internal_status'])

        for key in GoodExperiments:
            if GoodExperiments[key]:
                problemWriter.writerow([key, 'post-pipeline review'])
            else:
                releasedFiles.write(key)
                releasedFiles.write('\n')
                problemWriter.writerow([key, 'release ready'])


if __name__ == '__main__':
    main()
