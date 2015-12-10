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
                        help="single column list of object accessions")
    parser.add_argument('--query',
                        help="query of objects you want to process")
    parser.add_argument('--accession',
                        help="single accession to process")
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
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH the data.  Default is False")
    args = parser.parse_args()
    return args


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    accessions = []
    if args.query:
        temp = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        for obj in temp:
            accessions.append(obj.get("@id"))
    elif args.infile:
        accessions = [line.strip() for line in open(args.infile)]
    elif args.accession:
        accessions = [args.accession]
    else:
        print("No accessions to check!", file=sys.stderr)
        sys.exit(1)
    for acc in accessions:
        encodedcc.get_ENCODE(acc, connection)

if __name__ == '__main__':
        main()
