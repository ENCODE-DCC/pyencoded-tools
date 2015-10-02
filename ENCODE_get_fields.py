#!/usr/bin/env python
# -*- coding: latin-1 -*-
''' BASIC REPORTER SCRIPT
'''
import os.path
import argparse
import encodedcc

HEADERS = {'content-type': 'application/json'}
DEBUG_ON = False
EPILOG = '''Examples:

To use a different key from the default keypair file:

        %(prog)s --key submit
'''


def get_experiment_list(file, search, connection):
    objList = []
    if search == "NULL":
        f = open(file)
        objList = f.readlines()
        for i in range(0, len(objList)):
            objList[i] = objList[i].strip()
    else:
        set = encodedcc.get_ENCODE(search+'&limit=all&frame=embedded', connection)
        for i in range(0, len(set['@graph'])):
            objList.append(set['@graph'][i]['@id'])
    return objList


def get_antibody_approval(antibody, target):

        search = encodedcc.get_ENCODE('search/?searchTerm='+antibody+'&type=antibody_approval')
        for approval in search['@graph']:
            if approval['target']['name'] == target:
                return approval['status']
        return "UNKNOWN"


def main():

    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--infile', '-i', default='obList', help="File containing a list of ENCSRs.")
    parser.add_argument('--search', default='NULL', help="The search parameters.")
    parser.add_argument('--key', default='default', help="The keypair identifier from the keyfile.  Default is --key=default")
    parser.add_argument('--keyfile', default=os.path.expanduser("~/keypairs.json"), help="The keypair file.  Default is --keyfile=%s" %(os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--debug', default=False, action='store_true', help="Print debug messages.  Default is False.")
    parser.add_argument('--field', default='accession', help="The field to report.  Default is accession.")
    args = parser.parse_args()

    DEBUG_ON = args.debug
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    # Get list of objects we are interested in
    objList = get_experiment_list(args.infile, args.search, connection)
    for i in range(0, len(objList)):
        field = ''
        if objList[i] != '':
            ob = encodedcc.get_ENCODE(objList[i], connection)
            id = ob.get('@id')
            if args.field in ob:
                field = str(ob[args.field])
        else:
            id = objList[i]
        print('\t'.join([id, field]))

if __name__ == '__main__':
    main()
