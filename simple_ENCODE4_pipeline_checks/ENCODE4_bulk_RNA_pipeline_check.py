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
ENCODE4_BULK_RNA_PIPELINES = [
    '/pipelines/ENCPL862USL/',
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
            if ('in progress' in analyses_dict[acc]['status']) or (analyses_dict[acc]['date'] > analyses_dict[latest]['date'] and 'deleted' not in analyses_dict[acc]['status']):
                latest = acc 

    return latest


def check_encode4_bulk_rna_pipeline(exp_acc):
    experiment = requests.get(BASE_URL.format(exp_acc), auth=AUTH).json()

    print('------------------------------')
    print(exp_acc)
    print('------------------------------')
    bad_reason = []
    archiveAnalyses = {}
    archiveAnalyses[exp_acc] = []
    preferred_default_file_format = []
    preferred_default_output_type = set()
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
    skipped_ENC4_analyses_count = 0
    skipped_analyses_count = 0
    rep_count = len({
        rep['biological_replicate_number']
        for rep in experiment['replicates']
    })
    file_output_map = {}
    replicateObj = experiment.get('replicates', [])
    strandSpecificity = []
    libraries = []
    avgFragLength = False
    for rep in replicateObj:
        libraries.append(rep['library']['accession'])
        try:
            strandSpecificity.append(rep['library']['strand_specificity'])
        except:
            strandSpecificity.append('unstranded')
        try:
            if rep['library']['average_fragment_size']:
                avgFragLength = True
        except:
            continue

    if len(set(libraries)) != rep_count:
        rep_count = len(set(libraries))

    runType_allFiles = []
    runType = None
    fileObj = experiment.get('files', [])
    for f in fileObj:
        if 'reads' in f['output_type']:
            try:
                runType_allFiles.append(f['run_type']) 
            except:
                continue

    if len(set(runType_allFiles)) > 1:
        print('Multiple run types')
        bad_reason.append('Multiple run types')
    else:
        for item in runType_allFiles:
            runType = item

    stranded = False
    # determine strand information
    if len(set(strandSpecificity)) == 1:
        if 'unstranded' not in set(strandSpecificity):
            stranded = True
    else:
        print('Lacking or differing strand information')
        bad_reason.append('Differing strand information')
    
    expected_file_output_count = {
        'transcriptome alignments': rep_count,
        'alignments': rep_count,
        'gene quantifications': rep_count,
        'transcript quantifications': rep_count * 2,
    }

    if stranded:
        expected_file_output_count.update({
            'minus strand signal of unique reads': rep_count,
            'plus strand signal of unique reads': rep_count,
            'minus strand signal of all reads': rep_count,
            'plus strand signal of all reads': rep_count,
        })

        expected_preferred_default_file_format = ['bigWig', 'bigWig', 'tsv']
        expected_preferred_default_output_type = [
            'gene quantifications',
            'plus strand signal of unique reads', 
            'minus strand signal of unique reads'
        ]

        if runType == 'single-ended' and not avgFragLength:
            expected_file_output_count.update({
                    'transcript quantifications': rep_count,
            })      

    else:
        expected_file_output_count.update({
            'signal of unique reads': rep_count,
            'signal of all reads': rep_count,
        })

        expected_preferred_default_file_format = ['bigWig', 'tsv']
        expected_preferred_default_output_type = ['gene quantifications', 'signal of unique reads']

        if runType == 'single-ended' and not avgFragLength:
            expected_file_output_count.update({
                'transcript quantifications': rep_count,
            })  

    for analysis in analysisObj:
        
        # archive all other released analyses
        if analysis['status'] in ['released'] and analysis['accession'] != latest:
            archiveAnalyses[exp_acc].append(analysis['accession'])

        analysisStatus = ["released", "in progress", "archived"]
        if sorted(analysis['pipelines']) != ENCODE4_BULK_RNA_PIPELINES:
            skipped_analyses_count += 1
            continue

        if sorted(analysis['pipelines']) == ENCODE4_BULK_RNA_PIPELINES and analysis['accession'] != latest:
            skipped_ENC4_analyses_count += 1
            continue 

        print('Analysis object {} being checked'.format(analysis['accession']))

        if analysis.get('assembly') not in ['GRCh38', 'mm10']:
            print('Wrong assembly')
            bad_reason.append('Wrong assembly')
        if analysis.get('genome_annotation') not in ['V29', 'V21', 'M21']:
            print('Wrong genome annotation')
            bad_reason.append('Wrong genome annotation')
        for fid in analysis['files']:
            f_obj = requests.get(BASE_URL.format(fid), auth=AUTH).json()
            file_output_map.setdefault(f_obj['output_type'], 0)
            file_output_map[f_obj['output_type']] += 1
            if f_obj.get('preferred_default'):
                preferred_default_file_format.append(f_obj['file_format'])
                preferred_default_output_type.add(f_obj['output_type'])
        if sorted(
                preferred_default_file_format
            ) != sorted(expected_preferred_default_file_format):
                msg = 'Wrong preferred default file format'
                if rep_count == 1:
                    msg += '; unreplicated experiment'
                print(msg)
                bad_reason.append(msg)

        if stranded:
            if (
                len(preferred_default_output_type) != 3
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
        else:
            if (
                len(preferred_default_output_type) != 2
                or list(
                    preferred_default_output_type
                )[0] not in expected_preferred_default_output_type
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
    return bad_reason, serious_audits, archiveAnalyses


def get_parser():
    parser = argparse.ArgumentParser(
        description='Script to check result of ENCODE4 bulk RNA-seq processing on '
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
    SkippedExperiments = []
    patchAnalyses = {}
    for exp_acc in args.exp_accs:
        bad_reason, serious_audits, archiveAnalyses = check_encode4_bulk_rna_pipeline(
            exp_acc.strip()
        )
        status = ', '.join(bad_reason) or 'Good'
        if status == 'Good':
            experimentID = exp_acc.strip()
            GoodExperiments[experimentID] = sum(serious_audits.values())
        else:
            SkippedExperiments.append(exp_acc.strip())
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

    if len(SkippedExperiments) > 0:
        print('Excluding following datasets from the releasedPatch.txt file:')
        for item in SkippedExperiments:
            print(f'{item}')


if __name__ == '__main__':
    main()
