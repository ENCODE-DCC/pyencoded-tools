#!/usr/bin/env python
# -*- coding: latin-1 -*-
''' BASIC REPORTER SCRIPT
'''
import os.path
import argparse
import encodedcc

EPILOG = '''
To get multiple fields use the multifield argument:

        %(prog)s --infile filename --multifield fieldnames

    where the infile is a list of object identifiers
    and the multifield is a list of fields desired


To get a single field use the onefield argument:

        %(prog)s --infile filename --onefield field

    where onefield is a string containing the field name


To use a custom query for your object list:

        %(prog)s --query www.my/custom/url

    this can be used with multifield or onefield
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--infile',
                        help="File containing a list of ENCs as a column")
    parser.add_argument('--query',
                        help="A custom query to get accessions.")
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
    parser.add_argument('--multifield',
                        help="File of fieldnames with one per line")
    parser.add_argument('--onefield',
                        help="single field entered at command line")
    args = parser.parse_args()
    return args


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    encodedcc.get_fields(args, connection)

if __name__ == '__main__':
    main()
