#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os.path
import encodedcc
import sys

EPILOG = '''
This script opens a fastq and calculates the read length,
it can also print the header line of the fastq
currently unable to parse header for information such as machine name

        %(prog)s --infile file.txt
        %(prog)s --infile ENCFF000AAA
        %(prog)s --infile ENCFF000AAA,ENCFF000AAB,ENCFF000AAC

    Takes either a list of the file accessions, a single accession, \
    or comma separated list of accessions

        %(prog)s --query "/search/?type=File"

    Takes a query from which to get the list of files

        %(prog)s --header

    Prints the header line from the fastq
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--infile',
                        help="list of FASTQ file accessions, single accession \
                        or comma separated accession list")
    parser.add_argument('--query',
                        help="ENCODE query to get EXPERIMENT accessions from")
    parser.add_argument('--header',
                        help="Prints 'header' line from fastq.  Default is false",
                        action='store_true',
                        default=False)
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
    accessions = []
    if args.infile:
        if os.path.isfile(args.infile):
            accessions = [line.strip() for line in open(args.infile)]
        else:
            accessions = args.infile.split(",")
    elif args.query:
        data = []
        if "search" in args.query:
            data = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        else:
            data = [encodedcc.get_ENCODE(args.query, connection)]
        for exp in data:
            files = exp.get("files", [])
            for f in files:
                res = encodedcc.get_ENCODE(f, connection)
                f_type = res.get("file_format", "")
                if f_type == "fastq":
                    accessions.append(res["accession"])
    else:
        print("No accessions to check")
        sys.exit(1)
    for acc in accessions:
        link = "/files/" + acc + "/@@download/" + acc + ".fastq.gz"
        for header, sequence, qual_header, quality in encodedcc.fastq_read(connection, uri=link):
            if args.header:
                header = header.decode("UTF-8")
                print(header)
            else:
                sequence = sequence.decode("UTF-8")
                print(acc + "\t" + str(len(sequence)))

if __name__ == '__main__':
        main()
