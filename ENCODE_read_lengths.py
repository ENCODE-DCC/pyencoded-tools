import argparse
import os.path
import encodedcc
import sys

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--infile',
                        help="list of FASTQ file accessions")
    parser.add_argument('--accession',
                        help="single accession")
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
    parser.add_argument('--query',
                        help="ENCODE query to get EXPERIMENT accessions from")
    parser.add_argument('--header',
                        help="Prints 'header' line from fastq.  Default is false",
                        action='store_true',
                        default=False)
    args = parser.parse_args()
    return args


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    accessions = []
    if args.infile:
        accessions = [line.rstrip("\n") for line in open(args.infile)]
    elif args.query:
        data = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        for exp in data:
            files = exp.get("files", [])
            for f in files:
                res = encodedcc.get_ENCODE(f, connection)
                f_type = res.get("file_format", "")
                if f_type == "fastq":
                    accessions.append(res["accession"])
    elif args.accession:
        accessions = [args.accession]
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
