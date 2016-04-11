import argparse
import os.path
import encodedcc
import sys
from urllib.parse import quote

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
    data = encodedcc.get_ENCODE(args.query, connection).get("@graph")
    print("Experiment\tStatus\tControl\tStatus")
    for exp in data:
        if exp.get("possible_controls"):
            if exp["status"] != "released":
                c = exp["possible_controls"][0]
                control = encodedcc.get_ENCODE(c, connection)
                if control["status"] == "released":
                    print("{}\t{}\t{}\t{}".format(exp["accession"], exp["status"], control["accession"], control["status"]))
    '''
    query = "/search/?type=Experiment&assay_title=eCLIP&target.investigated_as=control"
    temp = encodedcc.get_ENCODE(query, connection).get("@graph", [])
    search = "/search/?type=Experiment&assay_title=eCLIP&target.name="
    print("Experiment\tControl\tBiosample\tTarget\tControlTarget")
    for exp in temp:
        target = exp.get("target")
        control_gene = encodedcc.get_ENCODE(target, connection).get("name")
        exp_gene = control_gene.split()[0] + "-human"
        #print(control_gene, exp_gene)
        eclips = encodedcc.get_ENCODE(search + exp_gene + "&target.investigated_as!=control", connection).get("@graph", [])
        controls = encodedcc.get_ENCODE(search + control_gene, connection).get("@graph", [])
        #print("controls", len(controls), "experiments", len(eclips))
        for e in eclips:
            for c in controls:
                if e.get("biosample_term_name") == c.get("biosample_term_name"):
                    patch_dict = {"possible_controls": [c["@id"]]}
                    if args.update:
                        print("Patching {} with possible_controls {}".format(e["@id"], c["@id"]))
                        encodedcc.patch_ENCODE(e["@id"], connection, patch_dict)
                    else:
                        print("{}\t{}\t{}\t{}\t{}".format(e["accession"], c["accession"], c["biosample_term_name"], exp_gene, control_gene))
    '''

if __name__ == '__main__':
        main()
