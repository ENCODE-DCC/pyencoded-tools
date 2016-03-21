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
    parser.add_argument('--query',
                        default="/search/?type=Experiment",
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
    status = "ungraded"
    for f in files:
        if f.get("output_category", "") != "raw data":
            print("shall we?")
            audits = d.get("audit", {})
            Error = False
            Not_Compliant = False
            Warn = False
            if any(audits):
                if audits.get("ERROR"):
                    Error = True
                if audits.get("NOT_COMPLIANT"):
                    Not_Compliant = True
                if audits.get("WARNING"):
                    Warn = True
            if Error and Warn and Not_Compliant:
                return "bronze"
            elif Error and Warn and not Not_Compliant:
                return "silver"
            elif not Error and not Warn and not Not_Compliant:
                return "gold"
            else:
                return "Error"
        else:
            status = "ungraded"
    return status


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on {}".format(connection.server))
    data = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
    # bronze = warning and not compliant, missing error
    # silver = warning, missing error and not compliant
    # gold = nothing, missing all 3
    for d in data:
        obj = encodedcc.get_ENCODE(d["@id"], connection, frame="page")
        status = audit_check(obj)
        print("{exp}\t{stat}".format(exp=d["accession"], stat=status))

if __name__ == '__main__':
        main()
