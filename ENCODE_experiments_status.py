import argparse
import os.path
import encodedcc
import requests
import urllib.parse
from time import sleep
import pandas as pd
from datetime import datetime

import sys
GET_HEADERS = {'accept': 'application/json'}


EPILOG = '''
For more details:

        %(prog)s --help
'''
'''
Example command:
python3 ENCODE_experiments_status.py --keyfile keypairs.json --key test  --query 'accession=ENCFF123ABC'
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--query', default='',
                        help="override the experiments search query, e.g. 'accession=ENCFF000ABC'")
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
    args = parser.parse_args()
    return args


def check_read_counts(read_count, threshold, raw_data, ex):
    if read_count < threshold:
        depth_flag = True
        raw_data['accession'].append(ex['accession'])
        raw_data['status'].append(ex['status'])
        raw_data['recommendation'].append('MORE SEQUENCING REQUIRED')
        return True
    return False


def check_audit_errors(ex):
    if ex.get('audit') and ('ERROR' in ex.get('audit') or 'NOT_COMPLIANT' in ex.get('audit')):
        return ex.get('audit')
    return None


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    query = args.query

    experiments_url = '/search/?type=Experiment&' + \
        'status=proposed&status=started&' + \
        'format=json&frame=page&' + \
        query
    experiments = encodedcc.get_ENCODE(experiments_url,
                                       connection)['@graph']
    print ('There are ' + str(len(experiments)) +
           ' experiments that should be inspected on the portal')

    raw_data = {'accession': [],
            'status': [],
            'recommendation':[]}
    mone = 0

    # read_depth definitions:
    min_depth = {}
    min_depth['ChIP-seq'] = 20000000
    min_depth['RAMPAGE'] = 10000000
    min_depth['shRNA knockdown followed by RNA-seq'] = 10000000
    min_depth['siRNA knockdown followed by RNA-seq'] = 10000000
    min_depth['single cell isolation followed by RNA-seq'] = 10000000
    min_depth['CRISPR genome editing followed by RNA-seq'] = 10000000
    min_depth['modENCODE-chip'] = 500000
    for ex in experiments:
        #print (ex['accession'])
        mone += 1
        if mone % 10 == 0:
            print (mone)
        replicates = set()
        replicates_reads = {}
        files = []
        if ex.get('replicates') and ex.get('files'):
            try:
                award_url = ex.get('award') + '?frame=object&format=json'
                award_obj = encodedcc.get_ENCODE(award_url, connection)
                for rep in ex.get('replicates'):
                    rep_url = rep + '?frame=object&format=json'
                    replicate_obj = encodedcc.get_ENCODE(rep_url, connection)
                    if (replicate_obj.get('status') not in ['deleted']):
                        replicates.add(replicate_obj['@id'])
                        replicates_reads[replicate_obj['@id']] = 0
                for file_name in ex.get('files'):
                    file_url = file_name + '?frame=object&format=json'
                    file_obj = encodedcc.get_ENCODE(file_url, connection)
                    if (file_obj.get('status') not in ['uploading', 'content error', 'upload failed']):
                        files.append(file_obj)
                        if file_obj.get('read_count') and file_obj.get('replicate'):
                            if not replicates_reads.get(file_obj.get('replicate')) is None:
                                replicates_reads[file_obj.get('replicate')] += file_obj.get('read_count')
            except requests.exceptions.RequestException as e:
                print ('ERROR while getting the object ' + str(e))
            else:
                submitted_replicates = set()
                for file_name in files:
                    if file_name.get('replicate'):
                        submitted_replicates.add(file_name.get('replicate'))
                # only in cases we have FASTQs for every replicate
                # check the read depth
                if not (replicates - submitted_replicates):
                    depth_flag = False
                    modENCODE_ChIP = (award_obj.get('rfa') == 'modENCODE' and \
                        ex['assay_term_name'] == 'ChIP-seq')
                    if (modENCODE_ChIP or \
                        (ex['assay_term_name'] in min_depth and \
                        award_obj.get('rfa') != 'modENCODE')):
                        assay_key = ex['assay_term_name']
                        if modENCODE_ChIP:
                            assay_key = 'modENCODE-chip'
                        for rep in replicates_reads:
                            depth_flag = check_read_counts(replicates_reads[rep], min_depth[assay_key], raw_data, ex)
                            if depth_flag:
                                print (str(award_obj.get('rfa')) + '\t'+ex['accession']+'\t' + rep + '\t' + str(replicates_reads[rep]))
                                break
                    
                    if not depth_flag:
                        errors = check_audit_errors(ex)
                        if not errors: 
                            raw_data['accession'].append(ex['accession'])
                            raw_data['status'].append(ex['status'])
                            
                            date_submitted = datetime.now().strftime('%Y-%m-%d')
                            patching_data = {
                                'status': 'submitted',
                                'date_submitted': date_submitted
                            }
                            raw_data['recommendation'].append(str(patching_data))
                            #encodedcc.patch_ENCODE(obj['uuid'],
                            #               connection, patching_data)
                        else:
                            raw_data['accession'].append(ex['accession'])
                            raw_data['status'].append(ex['status'])
                            
                            date_submitted = datetime.now().strftime('%Y-%m-%d')
                            raw_data['recommendation'].append(str(errors))

    df = pd.DataFrame(raw_data, columns = ['accession', 'status', 'recommendation'])
    df.to_csv('experiments_status.tsv', sep='\t')

if __name__ == '__main__':
    main()



