#!/usr/bin/env python
# -*- coding: latin-1 -*-
'''Read in a file of object, correction fields and patch each object'''

import argparse
import os.path
import encodedcc

EPILOG = '''
Input file should be a TSV (tab separated value) file with headers

accession   header1  header2     header3 ...
ENCSR000AAA value1   [list1,list2] value3  ...

Whatever data is used to identify the object (accession, uuid, alias)
goes in the accession column to be used for identification of object

Examples:

To PATCH data run with update comamnd:

        %(prog)s --update

To PATCH a single object, field, and data:

        %(prog)s --accession ENCSR000AAA --field assay_term_name --data ChIP-seq

For more details:

        %(prog)s --help
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
    parser.add_argument('--remove',
                        default=False,
                        action='store_true',
                        help="Patch to remove the value specified in the input \
                        file from the given field.  Requires --update to work. Default is False.")
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH the data.  Default is False")
    parser.add_argument('--accession',
                        help="Single accession/identifier to patch")
    parser.add_argument('--field',
                        help='Field for single accession')
    parser.add_argument('--data',
                        help='Data for single accession')
    parser.add_argument('--array',
                        help='Use if the single accession is an array')
    args = parser.parse_args()
    return args


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    encodedcc.patch_set(args, connection)

if __name__ == '__main__':
        main()
