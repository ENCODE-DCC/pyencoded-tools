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
    parser.add_argument('--search',
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
    for f in files:
        if f.get("output_category", "") == "raw data":
            status = "ungraded"
            return status
    audits = d.get("audit", {})
    temp = {"Error": False, "Not Compliant": False, "Warning": False}
    if any(audits):
        if audits.get("ERROR"):
            temp["Error"] = True
        if audits.get("NOT_COMPLIANT"):
            temp["Not Compliant"] = True
        if audits.get("WARNING"):
            temp["Warning"] = True
    if temp["Error"] and temp["Warning"] and temp["Not Compliant"]:
        status = "bronze"
    elif temp["Error"] and temp["Warning"] and not temp["Not Compliant"]:
        status = "silver"
    elif not temp["Error"] and not temp["Warning"] and not temp["Not Compliant"]:
        status = "gold"
    else:
        status = "Error"
    return status


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    data = encodedcc.get_ENCODE(args.search, connection).get("@graph", [])
    # bronze = warning and not compliant, missing error
    # silver = warning, missing error and not compliant
    # gold = nothing, missing all 3
    for d in data:
        obj = encodedcc.get_ENCODE(d["@id"], connection, frame="page")
        status = audit_check(obj)
        print("{exp}\t{stat}".format(exp=d["accession"], stat=status))

if __name__ == '__main__':
        main()
