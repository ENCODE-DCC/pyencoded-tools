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
    parser.add_argument('--object',
                        help="Either the file containing a list of ENCs as a column,\
                        a single accession by itself, or a comma separated list of identifiers")
    parser.add_argument('--query',
                        help="query of objects you want to process")
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
    if args.object:
        if os.path.isfile(args.object):
            accessions = [line.strip() for line in open(args.object)]
        else:
            accessions = args.object.split(",")
    elif args.query:
        if "search" in args.query:
            temp = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        else:
            temp = [encodedcc.get_ENCODE(args.query, connection)]
        if any(temp):
            for obj in temp:
                if obj.get("accession"):
                    accessions.append(obj["accession"])
                elif obj.get("uuid"):
                    accessions.append(obj["uuid"])
                elif obj.get("@id"):
                    accessions.append(obj["@id"])
                elif obj.get("aliases"):
                    accessions.append(obj["aliases"][0])
                else:
                    print("ERROR: object has no identifier", file=sys.stderr)
    if len(accessions) == 0:
        print("No accessions to check!", file=sys.stderr)
        sys.exit(1)
    print("Experiment\tFASTQ\tFastq run_type\tfastq paired_end\tcontrol\tcontrol run_type\tcontrol paired_end")
    for acc in accessions:
        exp = encodedcc.get_ENCODE(acc, connection)
        files = exp.get("files", [])
        for fi in files:
            file = encodedcc.get_ENCODE(fi, connection)
            if file.get("file_type", "") == "fastq":
                controlled_by = file.get("controlled_by", [])
                for con in controlled_by:
                    control = encodedcc.get_ENCODE(con, connection)
                    print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(exp.get("accession"), file.get("accession"), file.get("run_type"), file.get("paired_end"), control.get("accession"), control.get("run_type"), control.get("paired_end")))

if __name__ == '__main__':
        main()
