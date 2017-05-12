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

    experiments = encodedcc.get_ENCODE('search/?type=Experiment&assay_term_name=HiC&assay_term_name=eCLIP&assay_term_name=ATAC-seq',
                                       connection)['@graph']
    assay_types = []
    for ex in experiments:
        if ex['assay_term_name'] not in assay_types:
            assay_types.append(ex['assay_term_name'])

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
            print (str(file_number) + '\t' + random.choice(ex_dict[file_number]))
            mone += 1
            if mone == 5:
                break


if __name__ == '__main__':
    main()
