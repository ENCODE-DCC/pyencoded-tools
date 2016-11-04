import argparse
import os

EPILOG = '''
For more details:

        %(prog)s --help
'''
'''
Example commands:

python3 ENCODE_shoppinator.py --key https://test.encodedcc.org/ --infile accessions_list_file
python3 ENCODE_shoppinator.py --infile ENCBS123ABC,ENCBS123ABD,ENCBS124ABC

'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--key',
                        default='https://www.encodeproject.org/',
                        help="Server you want the shoppinator URL to be associated with.  \
                        Default is https://www.encodeproject.org/")
    parser.add_argument('--infile',
                        help="A comma separated list or datafile containing " +
                             "accessions list to be " +
                             "addded to the shopping cart URL")
    parser.add_argument('--object-type',
                        default='Experiment',
                        help="object type the list of accessions in the " +
                        "shoppinator belongs to")
    args = parser.parse_args()
    return args


def main():
    args = getArgs()
    server = args.key
    object_type = args.object_type

    infile = args.infile
    if infile:
        if os.path.isfile(infile):
            ACCESSIONS = [line.rstrip('\n') for line in open(infile)]
        else:
            ACCESSIONS = infile.split(",")

    object_types = {
        'Experiment': 'ENCSR',
        'Biosample': 'ENCBS',
        'MouseDonor': 'ENCDO',
        'HumanDonor': 'ENCDO',
        'FlyDonor': 'ENCDO',
        'WormDonor': 'ENCDO',
        'Pipeline': 'ENCPL',
        'File': 'ENCFF'
    }
    if object_type not in object_types:
        print ('ERROR - Unrecognized object type : ' + object_type +
               ', please specify type from the following list : ' +
               str(sorted(object_types.keys())))
    else:
        is_valid = True
        attach = ''
        for acc in ACCESSIONS:
            if acc.startswith(object_types[object_type]) is False:
                print ('ERROR - Accession ' + acc +
                       ' does not match specified object type ' + object_type)
                is_valid = False
            if len(acc) != 11:
                print ('ERROR - Accession ' + acc +
                       ' does not match ENCODE acession format ' +
                       '(ENCXY123ABC) length')
                is_valid = False
            attach += '&accession=' + acc
        if is_valid:
            print (server + 'search/?type=' + object_type + attach)

if __name__ == '__main__':
    main()
