import argparse
import os.path
import encodedcc
import requests
import urllib.parse
from time import sleep
import csv
import json
import sys

GET_HEADERS = {'accept': 'application/json'}
CORE_MARKS = ['H3K27ac', 'H3K27me3', 'H3K36me3', 'H3K4me1',
              'H3K4me3', 'H3K9me3']
EXPERIMENT_IGNORE_STATUS = ['deleted', 'revoked', 'replaced']
FILE_IGNORE_STATUS = ['deleted', 'revoked', 'replaced',
                      'upload failed', 'format check failed', 'uploading']

EPILOG = '''
For more details:

        %(prog)s --help
'''
'''
Example command:
python3 chip_seq_matrix.py --keyfile keypairs.json --key test --lab bing-ren --organism 'Mus musculus' --target 'histone' 
'''
def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--lab',
                        help="lab performing chip-seq experiments" +
                        "(either \'bing-ren\' or \'bradley-bernstein\')")
    parser.add_argument('--organism',
                        help="organism chip-seq experiments performed on " +
                        "(either \'Mus musculus\' or \'Homo sapiens\')")
    parser.add_argument('--target',
                        help="target.investigated_as of chip-seq experiments performed " +
                             "(either \'histone\' or \'transcription factor\')")
    parser.add_argument('--audit_matrix',
                        help="path to the audit matrix output file")
    parser.add_argument('--run_type_matrix',
                        help="path to the run_type output file")
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


def is_interesting(experiment):
    if experiment['status'] in ['revoked', 'replaced', 'deleted']:
        return False
    return True


def is_replicated(experiment):
    if 'replication_type' in experiment and \
       experiment['replication_type'] == 'unreplicated':
        return False
    if 'audit' in experiment:
        list_of_audits = []
        if 'NOT_COMPLIANT' in experiment['audit']:
            list_of_audits.extend(experiment['audit']['NOT_COMPLIANT'])
        if 'WARNING' in experiment['audit']:
            list_of_audits.extend(experiment['audit']['WARNING'])
        for au in list_of_audits:
            if au['category'] in ['unreplicated experiment']:
                return False
    return True


def is_antibody_eligible(experiment):
    if 'audit' in experiment:
        if 'NOT_COMPLIANT' in experiment['audit']:
            for au in experiment['audit']['NOT_COMPLIANT']:
                if au['category'] in ['not eligible antibody']:
                    return False
    return True


def is_not_missing_antibody(experiment):
    if 'audit' in experiment:
        if 'ERROR' in experiment['audit']:
            for au in experiment['audit']['ERROR']:
                if au['category'] in ['missing antibody']:
                    return False
    return True


def is_not_missing_controls(experiment):
    if 'audit' in experiment:
        if 'NOT_COMPLIANT' in experiment['audit']:
            for au in experiment['audit']['NOT_COMPLIANT']:
                if au['category'] in ['missing possible_controls',
                                      'missing controlled_by']:
                    return False
    return True


def is_not_mismatched_controlled_by(experiment):
    if 'audit' in experiment:
        if 'ERROR' in experiment['audit']:
            for au in experiment['audit']['ERROR']:
                if au['category'] in ['mismatched controlled_by']:
                    return False
    return True


def is_not_mismatched_controlled_by_run_type(experiment):
    if 'audit' in experiment:
        if 'WARNING' in experiment['audit']:
            for au in experiment['audit']['WARNING']:
                if au['category'] in ['mismatched controlled_by run_type']:
                    return False
    return True


def is_not_mismatched_controlled_by_read_length(experiment):
    if 'audit' in experiment:
        if 'WARNING' in experiment['audit']:
            for au in experiment['audit']['WARNING']:
                if au['category'] in ['mismatched controlled_by read length']:
                    return False
    return True


def is_not_missing_paired_with(experiment):
    if 'audit' in experiment:
        if 'NOT_COMPLIANT' in experiment['audit']:
            for au in experiment['audit']['NOT_COMPLIANT']:
                if au['category'] in ['missing paired_with']:
                    return False
    return True


def is_insufficient_read_depth(experiment):
    if 'audit' in experiment:
        list_of_audits = []
        if 'NOT_COMPLIANT' in experiment['audit']:
            list_of_audits.extend(experiment['audit']['NOT_COMPLIANT'])
        if 'WARNING' in experiment['audit']:
            list_of_audits.extend(experiment['audit']['WARNING'])
        for au in list_of_audits:
            if au['category'] in ['insufficient read depth']:
                return False
    return True


def is_insufficient_library_complexity(experiment):
    if 'audit' in experiment:
        if 'NOT_COMPLIANT' in experiment['audit']:
            for au in experiment['audit']['NOT_COMPLIANT']:
                if au['category'] in ['insufficient library complexity']:
                    return False
        if 'WARNING' in experiment['audit']:
            for au in experiment['audit']['WARNING']:
                if au['category'] in ['low library complexity']:
                    return False
    return True

def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    keypair = (key.authid, key.authpw)
    server = key.server
    connection = encodedcc.ENC_Connection(key)



    lab = '&lab.name=' + args.lab
    organism = '&replicates.library.biosample.donor.organism.scientific_name=' + \
               args.organism

    histone_experiments_pages = encoded_get(server +
                                            'search/?type=Experiment' +
                                            '&assay_term_name=ChIP-seq'
                                            '&award.rfa=ENCODE3' + organism +
                                            '&target.investigated_as=' + args.target + lab +
                                            '&format=json&frame=page&limit=all', keypair)['@graph']
    print ("retreived "+str(len(histone_experiments_pages)) + " experiment pages")

    histone_controls_pages = encoded_get(server +
                                         'search/?type=Experiment' +
                                         '&assay_term_name=ChIP-seq'
                                         '&award.rfa=ENCODE3' + organism +
                                         '&target.investigated_as=control' + lab +
                                         '&format=json&frame=page&limit=all', keypair)['@graph']
    print ("retreived "+str(len(histone_controls_pages)) + " control pages")

    histone_experiments_objects = encoded_get(server +
                                            'search/?type=Experiment' +
                                            '&assay_term_name=ChIP-seq'
                                            '&award.rfa=ENCODE3' + organism +
                                            '&target.investigated_as=' + args.target + lab +
                                            '&format=json&frame=embedded&limit=all', keypair)['@graph']
    print ("retreived "+str(len(histone_experiments_objects)) + " experiment objects")

    histone_controls_objects = encoded_get(server +
                                            'search/?type=Experiment' +
                                            '&assay_term_name=ChIP-seq'
                                            '&award.rfa=ENCODE3' + organism +
                                            '&target.investigated_as=control' + lab +
                                            '&format=json&frame=embedded&limit=all', keypair)['@graph']
    print ("retreived "+str(len(histone_controls_objects)) + " control objects")

    matrix = {}
    control_matrix = {}
    sample_types = set()
    marks = set()
    histone_experiments_dict = {}
    for entry in histone_experiments_pages:
        histone_experiments_dict[entry['accession']] = {'page': entry}
    for entry in histone_experiments_objects:
        histone_experiments_dict[entry['accession']]['object'] = entry
        sample = entry['biosample_term_name']
        mark = entry['target']['label']
        if mark not in matrix:
            matrix[mark] = {}
        if sample not in matrix[mark]:
            matrix[mark][sample] = []

        if 'aliases' in entry:
            matrix[mark][sample].append((entry['accession'], entry['aliases']))
        else:
            matrix[mark][sample].append((entry['accession'], 'NO ALIASES'))
        sample_types.add(sample)
        marks.add(mark)

    histone_controls_dict = {}
    for entry in histone_controls_pages:
        histone_controls_dict[entry['accession']] = {'page': entry}
    for entry in histone_controls_objects:
        histone_controls_dict[entry['accession']]['object'] = entry

        sample = entry['biosample_term_name']
        mark = 'control'
        if mark not in control_matrix:
            control_matrix[mark] = {}
        if sample not in control_matrix[mark]:
            control_matrix[mark][sample] = []

        if 'aliases' in entry:
            control_matrix[mark][sample].append((entry['accession'], entry['aliases']))
        else:
            control_matrix[mark][sample].append((entry['accession'], 'NO ALIASES'))
        sample_types.add(sample)
        marks.add(mark)

    mone = 0
    for ac in histone_experiments_dict:
        page = histone_experiments_dict[ac]['page']
        obj = histone_experiments_dict[ac]['object']
        mone += 1
        #  check only experiments that are not DELETED/REVOKED/REPLACED
        if is_interesting(obj):
            if mone % 10 == 0:
                print ('processed '+str(mone) + ' out of ' +
                       str(len(histone_experiments_dict.keys())))

            statuses = {'replication': [], 'antibody': [], 'control': [], 'files': [], 'qc': []}
            if is_replicated(obj) is False or is_replicated(page) is False:
                statuses['replication'].append('unreplicated')
            if is_antibody_eligible(page) is False:
                statuses['antibody'].append('not eligible antybody')
            if is_not_missing_antibody(page) is False:
                statuses['antibody'].append('missing antybody')
            if is_not_mismatched_controlled_by(page) is False:
                statuses['control'].append('mismatched controled_by')
            if is_not_mismatched_controlled_by_run_type(page) is False:
                statuses['control'].append('mismatched controled_by run_type')
            if is_not_mismatched_controlled_by_read_length(page) is False:
                statuses['control'].append('mismatched controled_by read_length')
            if is_not_missing_controls(page) is False:
                statuses['control'].append('missing control')
            if is_not_missing_paired_with(page) is False:
                statuses['files'].append('missing paired_with files')
            if is_insufficient_read_depth(page) is False:
                statuses['qc'].append('insufficient read depth')
            if is_insufficient_library_complexity(page) is False:
                statuses['qc'].append('insufficient library complexity')

            if is_not_missing_controls(page) is True and \
               is_not_mismatched_controlled_by(page) is True:
                not_encode_3_flag = False
                for entry in obj['possible_controls']:
                    control_accession = entry['accession']
                    if control_accession in histone_controls_dict:
                        control_page = histone_controls_dict[control_accession]['page']
                        if is_insufficient_read_depth(control_page) is False:
                            statuses['control'].append('insufficient read '
                                                       'depth in control')
                        if is_insufficient_library_complexity(control_page) is False:
                            statuses['control'].append('insufficient library '
                                                       'complexity in control')
                    else:
                        not_encode_3_flag = True
                if (not_encode_3_flag is True):
                    statuses['control'].append('non ENCODE3 control')

            histone_experiments_dict[ac]['statuses'] = statuses

            rep_dict = {}
            for file_id in obj['original_files']:
                file_object = encodedcc.get_ENCODE(file_id.split('/')[2], connection)
                if file_object['status'] in ['deleted', 'replaced', 'revoked']:
                    continue
                if file_object['file_format'] == 'fastq':
                    if 'replicate' in file_object:
                        bio_rep_number = file_object['replicate']['biological_replicate_number']
                        tec_rep_number = file_object['replicate']['technical_replicate_number']
                        key = (bio_rep_number,tec_rep_number)
                        if key not in rep_dict:
                            rep_dict[key] = set()
                        if 'read_length' in file_object and 'run_type' in file_object:
                            if file_object['run_type'] == 'single-ended':
                                record_val = str(file_object['read_length'])+'SE'
                            else:
                                record_val = str(file_object['read_length'])+'PE'
                            rep_dict[key].add(record_val)
            seq_info_string = ''
            for k in sorted(rep_dict.keys()):
                reps_string = ''
                for member in rep_dict[k]:
                    reps_string += member + ', '
                seq_info_string += 'REP' + str(k[0]) + '.' + str(k[1]) + ' ' +reps_string[:-2]+'\r'

            histone_experiments_dict[ac]['seq_info'] = seq_info_string

    mone = 0
    for ac in histone_controls_dict:
        mone += 1

        page = histone_controls_dict[ac]['page']
        obj = histone_controls_dict[ac]['object']
        if is_interesting(obj):
            if mone % 10 == 0:
                print ('processed '+str(mone) + ' out of ' + str(len(histone_controls_dict.keys())))
            statuses = {'replication': [], 'files': [], 'qc': []}
            if is_replicated(obj) is False or is_replicated(page) is False:
                statuses['replication'].append('unreplicated')
            if is_not_missing_paired_with(page) is False:
                statuses['files'].append('missing paired_with files')
            if is_insufficient_read_depth(page) is False:
                statuses['qc'].append('insufficient read depth')
            if is_insufficient_library_complexity(page) is False:
                statuses['qc'].append('insufficient library complexity')

        histone_controls_dict[ac]['statuses'] = statuses
        rep_dict = {}
        for file_id in obj['original_files']:     
            file_object = encodedcc.get_ENCODE(file_id.split('/')[2], connection)
            if file_object['status'] in ['deleted', 'replaced', 'revoked']:
                continue
            if file_object['file_format'] == 'fastq':
                #print (file_object['accession'])
                if 'replicate' in file_object:
                    bio_rep_number = file_object['replicate']['biological_replicate_number']
                    tec_rep_number = file_object['replicate']['technical_replicate_number']
                    key = (bio_rep_number,tec_rep_number)
                    if key not in rep_dict:
                        rep_dict[key] = set()
                    if 'read_length' in file_object and 'run_type' in file_object:
                        if file_object['run_type'] == 'single-ended':
                            record_val = str(file_object['read_length'])+'SE'
                        else:
                            record_val = str(file_object['read_length'])+'PE'
                        rep_dict[key].add(record_val)
        seq_info_string = ''
        for k in sorted(rep_dict.keys()):
            reps_string = ''
            for member in rep_dict[k]:
                reps_string += member + ', '
            seq_info_string += 'REP' + str(k[0]) + '.' + str(k[1]) + ' ' +reps_string[:-2]+'\r'
        histone_controls_dict[ac]['seq_info'] = seq_info_string
        #print (ac)
        #print (histone_controls_dict[ac]['seq_info'])
        
    # we have matrix dictionary for the matrix creation - each cell contains a lit of all the accessions
    # we have the histone_experiments_dict that for each accession has a list of statuses ['replication', 'antibody', control']

    if args.target == "histone":

        marks_to_print = ['control']
        marks_to_print.extend(CORE_MARKS)
        for m in marks:
            if m not in CORE_MARKS and m != 'control':
                marks_to_print.append(m)
    else:
        marks_to_print = ['control']
        for m in marks:
            if m != 'control':
                marks_to_print.append(m)

    #output = open("/Users/idan/Desktop/mat.csv", "w")
    with open(args.audit_matrix, 'wb') as output:
        fields = ['sample'] + marks_to_print
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for sample in sample_types:
            row = {'sample': sample}


            for mark in marks_to_print:
                if mark != 'control':
                    if sample in matrix[mark]:
                        total = len(matrix[mark][sample])

                        accessionStatuses = {}
                        aliases = {}
                        for (acc, al) in matrix[mark][sample]:
                            aliases[acc] = al
                            accessionStatuses[acc] = []
                            statuses = histone_experiments_dict[acc]['statuses']

                            for k in statuses:
                                if len(statuses[k]) > 0:
                                    statuses_string = ''
                                    for status in statuses[k]:
                                        statuses_string += '-' + status + '\r'
                                    accessionStatuses[acc].append(statuses_string)
                        cell_info = ''
                        for acc in accessionStatuses:
                            if len(accessionStatuses[acc]) < 1:

                                cell_info += acc + ' ' + histone_experiments_dict[acc]['object']['status'] + \
                                                   '\r' + str(aliases[acc])

                            else:
                                statuses_string = ''
                                for status in accessionStatuses[acc]:
                                        statuses_string += status
                                cell_info += acc + ' ' + histone_experiments_dict[acc]['object']['status'] + \
                                                   '\r' + str(aliases[acc]) + '\r' + \
                                                   statuses_string
                            cell_info += '\r\n'
                        row.update({mark: 'Experiments number : '+str(total)+'\r' +
                                   cell_info})
                    else:
                        row.update({mark: 'NONE'})
                else:
                    if sample in control_matrix[mark]:
                        total = len(control_matrix[mark][sample])

                        accessionStatuses = {}
                        aliases = {}
                        for (acc, al) in control_matrix[mark][sample]:
                            aliases[acc] = al
                            accessionStatuses[acc] = []
                            statuses = histone_controls_dict[acc]['statuses']

                            for k in statuses:
                                if len(statuses[k]) > 0:
                                    statuses_string = ''
                                    for status in statuses[k]:
                                        statuses_string += '-' + status + '\r'
                                    accessionStatuses[acc].append(statuses_string)
                        cell_info = ''
                        for acc in accessionStatuses:
                            if len(accessionStatuses[acc]) < 1:
                                cell_info += acc + ' ' + histone_controls_dict[acc]['object']['status'] + \
                                                   '\r' + str(aliases[acc])
                            else:
                                statuses_string = ''
                                for status in accessionStatuses[acc]:
                                        statuses_string += status
                                cell_info += acc + ' ' + histone_controls_dict[acc]['object']['status'] + \
                                                   '\r' + str(aliases[acc]) + '\r' + \
                                                   statuses_string
                            cell_info += '\r\n'
                        row.update({mark: 'Experiments number : '+str(total)+'\r' +
                                          cell_info})
                    else:
                        row.update({mark: 'NONE'})

            writer.writerow(row)


    with open(args.run_type_matrix, 'wb') as output:
        fields = ['sample'] + marks_to_print
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for sample in sample_types:
            row = {'sample': sample}


            for mark in marks_to_print:
                if mark != 'control':
                    if sample in matrix[mark]:
                        total = len(matrix[mark][sample])

                        accessionStatuses = {}
                        aliases = {}
                        for (acc, al) in matrix[mark][sample]:
                            aliases[acc] = al
                            accessionStatuses[acc] = []
                            statuses = histone_experiments_dict[acc]['statuses']

                            for k in statuses:
                                if len(statuses[k]) > 0:
                                    statuses_string = ''
                                    for status in statuses[k]:
                                        statuses_string += '-' + status + '\r'
                                    accessionStatuses[acc].append(statuses_string)
                        cell_info = ''
                        for acc in accessionStatuses:
                            cell_info += acc + ' ' + \
                                               histone_experiments_dict[acc]['object']['status'] + \
                                               '\r' + str(aliases[acc]) + \
                                               '\r' + \
                                               histone_experiments_dict[acc]['seq_info']
                           
                            cell_info += '\r\n'
                        row.update({mark: 'Experiments number : '+str(total)+'\r' +
                                   cell_info})
                    else:
                        row.update({mark: 'NONE'})
                else:
                    if sample in control_matrix[mark]:
                        total = len(control_matrix[mark][sample])

                        accessionStatuses = {}
                        aliases = {}
                        for (acc, al) in control_matrix[mark][sample]:
                            aliases[acc] = al
                            accessionStatuses[acc] = []
                            statuses = histone_controls_dict[acc]['statuses']

                            for k in statuses:
                                if len(statuses[k]) > 0:
                                    statuses_string = ''
                                    for status in statuses[k]:
                                        statuses_string += '-' + status + '\r'
                                    accessionStatuses[acc].append(statuses_string)
                        cell_info = ''
                        for acc in accessionStatuses:

                            cell_info += acc + ' ' + histone_controls_dict[acc]['object']['status'] + \
                                               '\r' + str(aliases[acc]) + '\r' + \
                                               histone_controls_dict[acc]['seq_info']
                           
                            cell_info += '\r\n'
                        row.update({mark: 'Experiments number : '+str(total)+'\r' +
                                          cell_info})
                    else:
                        row.update({mark: 'NONE'})

            writer.writerow(row)

if __name__ == '__main__':
    main()
