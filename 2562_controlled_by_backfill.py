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
            accessions.append(obj.get("@id"))
    elif args.infile:
        accessions = [line.strip() for line in open(args.infile)]
    elif args.accession:
        accessions = [args.accession]
    else:
        assert args.query or args.infile or args.accession, "No accessions to check!"
    data = []
    for acc in accessions:
        controlFiles = {}
        exp_bio_num = []
        con_bio_num = []
        obj = encodedcc.get_ENCODE(acc, connection, frame="embedded")
        for f in obj.get("files", []):
            if f.get("file_type") == "fastq":
                if f.get("biological_replicates"):
                    exp_bio_num += f["biological_replicates"]
                else:
                    print("Exp File", f.get("uuid"), "is missing biological_replicates")
        possible_controls = obj.get("possible_controls", [])
        if len(possible_controls) == 0:
            print("ERROR", obj["accession"], "has no possible_controls")
            continue
        elif len(possible_controls) == 1:
            control = encodedcc.get_ENCODE(possible_controls[0]["accession"], connection, frame="embedded")
            control_files = control.get("files", [])
            if len(control_files) == 0:
                print("Control", control["accession"], "has no files!")
            else:
                for f in control_files:
                    if f.get("file_type") == "fastq":
                        if f.get("biological_replicates"):
                            con_bio_num += f["biological_replicates"]
                        else:
                            print("Control File", f.get("uuid"), "is missing biological_replicates")
                    else:
                        pass
        else:
            print("ERROR", acc, "has", len(possible_controls), "possible_controls")
            continue

        ##### put the dictionary together ####
        controlFiles["accession"] = obj["accession"]
        controlFiles["exp_bio_num"] = exp_bio_num
        controlFiles["con_bio_num"] = con_bio_num
        data.append(controlFiles)

    print(data)

if __name__ == '__main__':
        main()
