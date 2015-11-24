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
            accessions.append(obj.get("accession"))
    elif args.infile:
        accessions = [line.strip() for line in open(args.infile)]
    elif args.accession:
        accessions = [args.accession]
    else:
        assert args.query or args.infile or args.accession, "No accessions to check!"

    for acc in accessions:
        exp_fastqs = {}
        con_fastqs = {}
        print("Experiment", acc)
        obj = encodedcc.get_ENCODE(acc, connection, frame="embedded")
        target =  obj.get("target", {})
        if target.get("Label", "") == "Control":
            print(acc, "is a control experiment")
        for f in obj.get("files", []):
            if f.get("file_type") == "fastq":
                if f.get("biological_replicates"):
                    exp_fastqs[f["accession"]] = f["biological_replicates"][0]
                else:
                    print("Exp File", f["accession"], "is missing biological_replicates")
        possible_controls = obj.get("possible_controls", [])
        if len(possible_controls) == 0:
            print("ERROR", obj["accession"], "has no possible_controls")
        elif len(possible_controls) == 1:
            control = encodedcc.get_ENCODE(possible_controls[0]["accession"], connection, frame="embedded")
            control_files = control.get("files", [])
            if len(control_files) == 0:
                print("Control", control["accession"], "has no files!")
            else:
                for f in control_files:
                    if f.get("file_type") == "fastq":
                        if f.get("biological_replicates"):
                            con_fastqs[f["accession"]] = f["biological_replicates"][0]
                        else:
                            print("Control File", f["accession"], "is missing biological_replicates")
        else:
            print("ERROR", acc, "has", len(possible_controls), "possible_controls")
        # here we check the two dictionaries
        temp = []
        for e in exp_fastqs.keys():
            for c in con_fastqs.keys():
                if exp_fastqs[e] == con_fastqs[c]:
                    temp.append(c)
                    #print(e, c, "have same biological_replicates value")
        print(e, temp)
        print("Experiment", exp_fastqs)
        print("Control", con_fastqs)

if __name__ == '__main__':
        main()
