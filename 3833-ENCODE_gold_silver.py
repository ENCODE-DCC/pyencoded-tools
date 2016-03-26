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
                        help="input optional search url")
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


def audit_check(d):
    files = d.get("files", [])
    status = "ungradable"
    for f in files:
        if f.get("output_category", "") != "raw data":
            audits = d.get("audit", {})
            if audits.get("ERROR"):
                return "unreleasable"
            elif audits.get("NOT_COMPLIANT"):
                return "bronze"
            elif audits.get("WARNING"):
                return "silver"
            else:
                return "gold"
    return status


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
                else:
                    print("ERROR: object has no identifier", file=sys.stderr)
    if len(accessions) == 0:
        print("No accessions to check!", file=sys.stderr)
        sys.exit(1)
    # E | NC | W
    # 0 | 0  | 0 = gold
    # 0 | 0  | 1 = silver
    # 0 | 1  | X = bronze
    # 1 | X  | X = unreleaseable
    # failing raw data check = ungradable
    print("accession\tvalidation_status")
    for acc in accessions:
        obj = encodedcc.get_ENCODE(acc, connection, frame="page")
        if obj["status"] != "replaced":
            status = audit_check(obj)
            print("{exp}\t{stat}".format(exp=acc, stat=status))

if __name__ == '__main__':
        main()
