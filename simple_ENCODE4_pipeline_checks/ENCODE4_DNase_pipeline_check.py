#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import csv
import requests
import datetime

AUTH = (os.environ.get("DCC_API_KEY"), os.environ.get("DCC_SECRET_KEY"))
BASE_URL = 'https://www.encodeproject.org/{}/?format=json'
ENCODE4_DNASE_PIPELINES = ['/pipelines/ENCPL848KLD/']
PREFERRED_DEFAULT_FILE_FORMAT = ['bigWig', 'bed', 'bigBed']
PREFERRED_DEFAULT_OUTPUT_TYPE = [
    'read-depth normalized signal', 'peaks'
]


def get_latest_analysis(analyses):

    # preprocessing
    if not analyses:
        return None

    analyses_dict = {}
    for a in analyses:
        analysis = requests.get(BASE_URL.format(a['accession']), auth=AUTH).json()
        date_created = analysis['date_created'].split('T')[0]
        date_obj = datetime.datetime.strptime(date_created, '%Y-%m-%d')
        analyses_dict[analysis['accession']] = {
            'date': date_obj,
            'pipeline_rfas': analysis['pipeline_award_rfas'],
            'pipeline_labs': analysis['pipeline_labs'],
            'status': analysis['status'],
            'assembly': analysis['assembly']
        }

    latest = None
    assembly_latest = False

    for acc in analyses_dict.keys():
        archivedFiles = False
        encode_rfa = False
        assembly_latest = False

        if not latest:
            latest = acc

        if 'ENCODE4' in analyses_dict[acc]['pipeline_rfas']:
            latest = acc
            if ('in progress' in analyses_dict[acc]['status']) or (analyses_dict[acc]['date'] > analyses_dict[latest]['date']):
                latest = acc

    return latest


def check_encode4_dnase_pipeline(exp_acc):
    experiment = requests.get(BASE_URL.format(exp_acc), auth=AUTH).json()
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
        print('Has {} ERROR audits'.format(serious_audits['ERROR']))
    if serious_audits['NOT_COMPLIANT']:
        print('Has {} NOT_COMPLIANT audits'.format(
            serious_audits['NOT_COMPLIANT']
        ))
    print('Number of original files: {}'.format(
        len(experiment['original_files'])
    ))
    analysisObj = experiment.get('analyses', [])
    latest = get_latest_analysis(analysisObj)
    print('Number of analyses: {}'.format(len(analysisObj)))
    print('File count in analyses: {}'.format(list(
        len(analysis['files']) for analysis in analysisObj
    )))
    skipped_analyses_count = 0
    skipped_ENC4_analyses_count = 0
    preferred_default_file_format = []
    preferred_default_output_type = set()

    rep_count = len({
        rep['biological_replicate_number']
        for rep in experiment['replicates']
    })
    file_output_map = {}
    expected_file_output_count = {
        ('unfiltered alignments', 'bam'): rep_count,
        ('alignments', 'bam'): rep_count,
        ('peaks', 'bed'): rep_count * 2,
        ('peaks', 'bigBed'): rep_count * 2,
        ('peaks', 'starch'): rep_count,
        ('FDR cut rate', 'bed'): rep_count,
        ('FDR cut rate', 'bigBed'): rep_count,
        ('read-depth normalized signal', 'bigWig'): rep_count,
        ('footprints', 'bed'): rep_count,
        ('footprints', 'bigBed'): rep_count,
    }
    for analysis in analysisObj:
        if analysis['status'] in ['released'] and analysis['accession'] != latest:
            archiveAnalyses[exp_acc].append(analysis['accession'])

        if sorted(analysis['pipelines']) != ENCODE4_DNASE_PIPELINES:
            skipped_analyses_count += 1
            continue
        if sorted(analysis['pipelines']) == ENCODE4_DNASE_PIPELINES and analysis['accession'] != latest:
            skipped_ENC4_analyses_count += 1
            continue

        print('Analysis object {} was checked'.format(analysis['accession']))
        if analysis.get('assembly') not in ['GRCh38', 'mm10']:
            print('Wrong assembly')
            bad_reason.append('Wrong assembly')
        if analysis.get('genome_annotation'):
            print('Has genome annotation')
            bad_reason.append('Has genome annotation')
        for fid in analysis['files']:
            f_obj = requests.get(BASE_URL.format(fid), auth=AUTH).json()
            file_output_map.setdefault(
                (f_obj['output_type'], f_obj['file_format']), 0
            )
            file_output_map[(f_obj['output_type'], f_obj['file_format'])] += 1
            if f_obj.get('preferred_default'):
                preferred_default_file_format.append(f_obj['file_format'])
                preferred_default_output_type.add(f_obj['output_type'])
        if (
            rep_count == 1
            and not preferred_default_file_format
            and not preferred_default_output_type
        ):
            print('Unreplicated experiment with no preferred default')
        else:
            if sorted(
                preferred_default_file_format
            ) != sorted(PREFERRED_DEFAULT_FILE_FORMAT):
                print(sorted(preferred_default_file_format))
                print('Wrong preferred default file format')
                bad_reason.append('Wrong preferred default file format')
            if (
                len(preferred_default_output_type) != 2
                or list(
                    preferred_default_output_type
                )[0] not in PREFERRED_DEFAULT_OUTPUT_TYPE
                or list(
                    preferred_default_output_type
                )[1] not in PREFERRED_DEFAULT_OUTPUT_TYPE
            ):

                print('Wrong preferred default file output type')
                bad_reason.append('Wrong preferred default file output type')
        if file_output_map != expected_file_output_count:
            print('Wrong file output type map')
            bad_reason.append('Wrong file output type map')
            print('Has {}'.format(str(file_output_map)))
            print('Expect {}'.format(str(expected_file_output_count)))
    if skipped_analyses_count == len(analysisObj):
        print('No ENCODE4 analysis found')
        bad_reason.append('No ENCODE4 analysis found')
    if skipped_analyses_count:
        print('Skipped {} non-ENCODE4 uniform analyses'.format(
            skipped_analyses_count
        ))
    print('')
    return bad_reason, serious_audits, archiveAnalyses


def get_parser():
    parser = argparse.ArgumentParser(
        description='Script to check result of ENCODE4 ATAC-seq processing on '
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
        bad_reason, serious_audits, archiveAnalyses = check_encode4_dnase_pipeline(
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
        print(GoodExperiments)
        for key in GoodExperiments:
            if GoodExperiments[key]:
                problemWriter.writerow([key, 'post-pipeline review'])
            else:
                releasedFiles.write(key)
                releasedFiles.write('\n')
                problemWriter.writerow([key, 'release ready'])


if __name__ == '__main__':
    main()
