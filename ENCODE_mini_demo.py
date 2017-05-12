import argparse
import os.path
import encodedcc
import datetime
import sys
import random

GET_HEADERS = {'accept': 'application/json'}


EPILOG = '''
For more details:

        %(prog)s --help
'''
'''
Example command:
python3 ENCODE_replaced_cleaner.py --keyfile keypairs.json --key test
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    args = parser.parse_args()
    return args


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    experiments = encodedcc.get_ENCODE('search/?type=Experiment&assay_term_name=HiC&assay_term_name=RRBS&assay_term_name=ATAC-seq',
                                       connection)['@graph']
    assay_types = []
    assay_statuses = []
    for ex in experiments:
        if ex['assay_term_name'] not in assay_types:
            assay_types.append(ex['assay_term_name'])
        if ex['status'] not in assay_statuses:
            assay_statuses.append(ex['status'])

    accessions_set = set()
    for assay_type in assay_types:
        print (assay_type)
        experiments = encodedcc.get_ENCODE('search/?type=Experiment&assay_term_name=' + assay_type,
                                           connection)['@graph']
        ex_dict = {}
        for ex in experiments:
            if len(ex['original_files']) not in ex_dict:
                ex_dict[len(ex['original_files'])] = [ex['accession']]
            else:
                ex_dict[len(ex['original_files'])].append(ex['accession'])

        mone = 0

        for file_number in sorted(ex_dict.keys(), reverse=True):
            print (str(file_number))
            for i in range(3):
                accessions_set.add(random.choice(ex_dict[file_number]))

            mone += 1
            if mone == 4:
                break

    # at this point we have representatives of different assays
    for assay_type in assay_types:
        for status in assay_statuses:
            experiments = encodedcc.get_ENCODE('search/?type=Experiment&assay_term_name=' + assay_type + '&status=' + status,
                                               connection)['@graph']
            if (experiments):
                print (assay_type + '\t' + status)
                for i in range(5):
                    random_experiment = random.choice(experiments)
                    if status == 'replaced':
                        replacement = encodedcc.get_ENCODE(random_experiment['accession'],
                                                           connection)
                        if replacement:
                            accessions_set.add(replacement['accession'])
                    accessions_set.add(random_experiment['accession'])


    # at this point we have representatives of different statuses of different assays

    series = encodedcc.get_ENCODE('search/?type=Series',
                                  connection)['@graph']
    series_types = []
    for s in series:
        if s['@type'][0] not in series_types:
            series_types.append(s['@type'][0])

    for series_type in series_types:
        print (series_type)
        series = encodedcc.get_ENCODE('search/?type=' + series_type,
                                      connection)['@graph']
        for i in range(3):
            accessions_set.add(random.choice(series)['accession'])

    # at this point we have representatives of different series
    antibodies = encodedcc.get_ENCODE('search/?type=AntibodyLot',
                                      connection)['@graph']
    antibody_statuses = []
    for a in antibodies:
        if a['status'] not in antibody_statuses:
            antibody_statuses.append(a['status'])
    for status in antibody_statuses:
        print (status)
        antibodies = encodedcc.get_ENCODE('search/?type=AntibodyLot&status=' + status,
                                          connection)['@graph']
        for i in range(20):
            accessions_set.add(random.choice(antibodies)['accession'])


    # at this point we have representatives of different antibodies statuses


    print (accessions_set)
if __name__ == '__main__':
    main()
