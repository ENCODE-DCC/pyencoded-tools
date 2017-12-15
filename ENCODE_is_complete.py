#!/usr/bin/env python
# -*- coding: latin-1 -*-
''' Prepare a summary for different datatypes for ENCODE3
'''
import os.path
import argparse
import encodedcc
import collections
from encodedcc import get_ENCODE

EPILOG = '''
This takes in a list of accessions and returns a status report
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--infile',
                        help="A minimum two column list with identifier and value to \
                        patch")
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH the data.  Default is False")
    parser.add_argument('--accession',
                        help="Single accession/identifier to patch")
    parser.add_argument('--query',
                        help="A custom query to get accessions.")

    args = parser.parse_args()
    return args


def get_experiment_list(file, search, connection):

    objList = []
    if search is None:
        f = open(file)
        objList = f.readlines()
        for i in range(0, len(objList)):
            objList[i] = objList[i].strip()
    else:
        col = get_ENCODE(search, connection, frame='page')
        for i in range(0, len(col['@graph'])):
            # print set['@graph'][i]['accession']
            objList.append(col['@graph'][i]['@id'])
            # objList.append(set['@graph'][i]['uuid'] )

    return objList


def get_replicate_count(obj):
    reps = []
    for rep in obj.get('replicates'):
        reps.append(rep['biological_replicate_number'])
    return list(set(reps))


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    assemblies = ['hg19', 'GRCh38']
    summary = []

    if args.infile is not None and 'ENCSR' in args.infile:
        objList = [args.infile]
    else:
        objList = get_experiment_list(args.infile, args.query, connection)

    for obj_id in objList:
        results = {}

        obj = get_ENCODE(obj_id, connection, frame='page')

        # Get basic info
        reps = get_replicate_count(obj)
        results['rep_count'] = len(reps)
        results['status'] = obj['status']
        results['internal_status'] = obj['internal_status']
        results['award'] = obj['award'].get('rfa')
        results['peaks'] = {}
        results['mapping'] = {}
        results['unarchived_files'] = []
        results['status issues'] = []
        results['accession'] = obj['accession']

        # Get audits
        for level in ['WARNING', 'ERROR', 'NOT_COMPLIANT', 'INTERNAL_ACTION']:
            if obj['audit'].get(level):
                results[level] = len(obj['audit'].get(level))

        # Get status issues
        actions = obj['audit'].get('INTERNAL_ACTION')
        if actions:
            status_issues = [i for i in actions if i['category'] in [
                'experiment not submitted to GEO', 'mismatched file status', 'mismatched status']]
            results['status issues'] = status_issues

        # Inspect files
        query = "/search/?type=File&dataset=/experiments/"+obj['accession']+'/'
        r =  get_ENCODE(query, connection, frame='embedded')
        all_files = r['@graph']

        good_files = [f for f in all_files
                      if f['status'] in ['released', 'in progress']]
        fastqs = [f for f in obj['files'] if f['status']
                  in ['released', 'in progress']]
        print("There are files in this experiment:", len(all_files))
        print("There are good files in this experiment:", len(good_files))
        # look for unarchived processed files from other labs
        processed_files = [f for f in all_files
                           if f['file_format'] != 'fastq']
        external_files = [f for f in processed_files if (
            f['lab']['name'] != 'encode-processing-pipeline')]
        unarchived_files = [f for f in external_files if (
            f['status'] != 'archived')]
        results['unarchived_files'] = unarchived_files

        for assembly in assemblies:
            replicates = []
            file_list = [f for f in good_files if f.get(
                'assembly') == assembly]
            for rep in reps:
                rep_obj = {'rep': rep}
                file_list_rep = [f for f in file_list if rep in f.get(
                    'biological_replicates')]
                aligns = [f for f in file_list_rep if f.get(
                    'output_type') == 'alignments']
                rep_obj['aligns'] = len(aligns)
                raw_aligns = [f for f in file_list_rep if f.get(
                    'output_type') == 'unfiltered alignments']
                rep_obj['raws'] = len(raw_aligns)
                replicates.append(rep_obj)
            failing_replicates = [f for f in replicates if f['aligns'] == 0]
            if len(failing_replicates) is 0:
                results['mapping'][assembly] = True
            elif len(replicates) == len(failing_replicates):  # They all fail
                results['mapping'][assembly] = False
            else:
                results['mapping'][assembly] = []
                for rep in failing_replicates:
                    results['mapping'][assembly].append(rep['rep'])

            peaks = [f for f in file_list if f.get('output_type') == 'peaks']
            if len(peaks) > 0:
                results['peaks'][assembly] = True
            else:
                results['peaks'][assembly] = False

        summary.append(results)

    unarchived_list = [r for r in summary if len(r['unarchived_files']) > 0]
    print('These experiments have unarchived files', len(unarchived_list))
    for item in unarchived_list:
        print(item['accession'])
    print('')
    print('')

    exps_mismatched_states = [
        r for r in summary if len(r['status issues']) > 0]
    print('These experiments have mismatched states',
          len(exps_mismatched_states))
    for item in exps_mismatched_states:
        print(item['accession'])
    print('')
    print('')

    # not_mapped_GRCh38 = [r for r in summary if r['missing_aligns']['GRCh38'] is False]

    exps_missing_hg38_mapping = [
        r for r in summary if r['mapping']['GRCh38'] is False]
    print('These experiments are missing GRCh38 mapping for all replicates', len(
        exps_missing_hg38_mapping))
    for item in exps_missing_hg38_mapping:
        print(item['accession'], item['status'], item['internal_status'])
    print('')
    print('')

    exps_partial_hg38_mapping = [r for r in summary if r['mapping']
                                 ['GRCh38'] is not False and r['mapping']['GRCh38'] is not True]
    print('These experiments are missing GRCh38 mapping for some replicates', len(
        exps_partial_hg38_mapping))
    for item in exps_partial_hg38_mapping:
        print(item['accession'], item['status'],
              item['internal_status'], item['mapping']['GRCh38'])
    print('')
    print('')

    exps_missing_hg38_peaks = [
        r for r in summary if r['peaks']['GRCh38'] is False]
    exps_missing_hg38_peaks_but_have_mapping = [f for f in exps_missing_hg38_peaks if f['peaks']
                                                ['GRCh38'] is False and f not in exps_missing_hg38_mapping and f not in exps_partial_hg38_mapping]
    print('These experiments are missing GRCh38 peaks but having all mappings', len(
        exps_missing_hg38_peaks_but_have_mapping))
    for item in exps_missing_hg38_peaks:
        print(item['accession'], item['status'], item['internal_status'])
    print('')
    print('')

    exps_missing_hg19_mapping = [
        r for r in summary if r['mapping']['hg19'] is False]
    print('These experiments are missing hg19 mapping for all replicates',
          len(exps_missing_hg19_mapping))
    for item in exps_missing_hg19_mapping:
        print(item['accession'], item['status'], item['internal_status'])
    print('')
    print('')

    exps_partial_hg19_mapping = [r for r in summary if r['mapping']
                                 ['hg19'] is not False and r['mapping']['hg19'] is not True]
    print('These experiments are missing hg19 mapping for some replicates',
          len(exps_partial_hg19_mapping))
    for item in exps_partial_hg19_mapping:
        print(item['accession'], item['status'],
              item['internal_status'], item['mapping']['hg19'])
    print('')
    print('')

    exps_missing_hg19_peaks = [r for r in summary if r['peaks']['hg19']
                               is False and r not in exps_missing_hg19_mapping and r not in exps_partial_hg19_mapping]
    print('These experiments are missing hg19 peaks',
          len(exps_missing_hg19_peaks))
    for item in exps_missing_hg19_peaks:
        print(item['accession'], item['status'],
              item['internal_status'], 'warnings:', item.get('WARNING'))
    print('')
    print('')


if __name__ == '__main__':
    main()
