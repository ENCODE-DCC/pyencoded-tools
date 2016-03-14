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


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    data = encodedcc.get_ENCODE(args.search, connection, frame="page").get("@graph", [])
    for d in data:
        audits = d.get("audit", {})
        status = ""
        if any(audits):
            if audits.get("ERROR") is None:
                # no errors gets bronze award
                status = "bronze"
                if audits.get("NOT COMPLIANT") is None:
                    # no errors and no not compliants gets silver
                    status = "silver"
                    if audits.get("WARNING") is None:
                        # no errors, not compliants, or warnings gets gold
                        status = "gold"
        else:
            # if there are no audits at all of course it qualifies for gold
            status = "gold"
        print("{exp}\t{stat}".format(exp=d["accession"], stat=status))

if __name__ == '__main__':
        main()
