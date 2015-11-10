import argparse
import os.path
import encodedcc

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
                        help="")
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
    parser.add_argument('--query')
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
        for obj in data:
            accessions.append(data["accession"])
    for acc in accessions:
        for header, sequence, qual_header, quality in encodedcc.fastq_read(connection, uri=acc):
            print(acc, len(sequence))

if __name__ == '__main__':
        main()
