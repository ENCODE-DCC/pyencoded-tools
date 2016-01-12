#!/usr/bin/env python
# -*- coding: latin-1 -*-
import os.path
import argparse
import encodedcc

EPILOG = '''
To get multiple objects use the '--object' argument
and provide a file with the list of object identifiers

        %(prog)s --object filenames.txt

    this can take accessions, uuids, @ids, or aliases

To get a single object use the '--object' argument
and use the object's identifier

        %(prog)s --object ENCSR000AAA
        %(prog)s --object 3e6-some-uuid-here-e45
        %(prog)s --object this-is:an-alias

To get multiple fields use the '--field' argument
and feed it a file with the list of fieldnames

        %(prog)s --field fieldnames.txt

    this should be a single column file

To get a single field use the field argument:

        %(prog)s --field status

    where field is a string containing the field name

To get embedded field values (such as target name from an experiment):

        %(prog)s --field target.title
    this can also get embedded values from lists

        %(prog)s --field files.status

To use a custom query for your object list:

        %(prog)s --query www.my/custom/url

    this can be used with either useage of the '--field' option

Output prints in format of fieldname:object_type for non-strings

    Ex: accession    read_length:int    documents:list
        ENCSR000AAA  31                 [document1,document2]

    integers  ':int'
    lists     ':list'
    string are the default and do not have an identifier
***please note that list type fields will show only unique items***

        %(prog)s --field files.status --object ENCSR000AAA

    accession       file.status:list
    ENCSR000AAA     ['released']

possible output even if multiple files exist in experiment

To show all possible outputs from a list type field
use the '--listfull' argument

        %(prog)s --field files.status --listfull

    accession       file.status:list
    ENCSR000AAA     ['released', 'released', 'released']


    *** ENCODE_collection useage and functionality  ***
%(prog)s has ported over some functions of ENCODE_collection
and now supports the '--collection' and '--allfields' options

Useage for '--allfields':

        %(prog)s --object ENCSR000AAA --allfields

    accession    status    files        award ...
    ENCSR000AAA  released  [/files/...] /awards/...

The '--allfields' option can be used with any of the commands,
it returns all fields at the frame=object level,
it also overrides any other --field option


Useage for '--collection':

        %(prog)s --collection Experiment --status

    accession    status
    ENCSR000AAA  released

The  '--collection' option can be used with or without the '--es' option
the '--es' option allows the script to search using elastic search,
which is slightly faster than the normal table view used
However, it may not posses the latest updates to the data and may not be
preferable to your application
'--collection' also overrides any other '--object' option and so but it
can be combined with any of the '--field' or '--allfields' options

NOTE: while '--collection' should work with the '--field' field.embeddedfield
functionality I cannot guarantee speed when running due to embedded
objects being extracted

'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--object',
                        help="Either the file containing a list of ENCs as a column\
                        or this can be a single accession by itself")
    parser.add_argument('--query',
                        help="A custom query to get accessions.")
    parser.add_argument('--field',
                        help="Either the file containing single column of fieldnames\
                        or the name of a single field")
    parser.add_argument('--listfull',
                        help="Normal list-type output shows only unique items\
                        select this to list all values even repeats. Default is False",
                        default=False,
                        action='store_true')
    parser.add_argument('--allfields',
                        help="Overrides other field options and gets all fields\
                        from the frame=object level. Default is False",
                        default=False,
                        action='store_true')
    parser.add_argument('--collection',
                        help="Overrides other object options and returns all\
                        objects from the selected collection")
    parser.add_argument('--es',
                        help="Used for collections, uses elastic search instead of table view",
                        default=False,
                        action='store_true')
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


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    output = encodedcc.GetFields(connection, args)
    output.get_fields()

if __name__ == '__main__':
    main()
