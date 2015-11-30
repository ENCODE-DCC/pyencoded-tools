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
    data = []
    for acc in accessions:
        exp_fastqs = {}
        con_fastqs = {}
        print("Experiment", acc)
        obj = encodedcc.get_ENCODE(acc, connection, frame="embedded")
        target = obj.get("target", {})
        if target.get("label", "") == "Control":
            print(acc, "is a control experiment")
        for f in obj.get("files", []):
            if f.get("file_type") == "fastq":
                if f.get("biological_replicates"):
                    biorep = str(f["biological_replicates"][0])
                    pair = str(f.get("paired_end"))
                    biokey = biorep + "-" + pair
                    exp_fastqs[f["accession"]] = biokey
                else:
                    print("Exp File", f["accession"], "is missing biological_replicates")
        possible_controls = obj.get("possible_controls", [])
        exp_replicates = obj.get("replicates", [])
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
                            biorep = str(f["biological_replicates"][0])
                            pair = str(f.get("paired_end"))
                            biokey = biorep + "-" + pair
                            con_fastqs[f["accession"]] = biokey
                        else:
                            print("Control File", f["accession"], "is missing biological_replicates")
        else:
            if len(exp_replicates) == len(possible_controls):
                # https://www.encodeproject.org/experiments/ENCSR757IIU/
                # biological replicate has a biosample, match the exp biosample to control biosample
                # if they match then the fastq of the control will match the fastq of the experiment based on bio_rep_num
                control_data = {}
                for c in possible_controls:
                    con = encodedcc.get_ENCODE(c["accession"], connection, frame="embedded")
                    for rep in con.get("replicates", []):
                        con_bio_acc = rep["library"]["biosample"]["accession"]
                        con_bio_num = rep["biological_replicate_number"]
                        for fi in c.get("files", []):
                            f = encodedcc.get_ENCODE(fi, connection, frame="embedded")
                            if f.get("file_type", "") == "fastq":
                                con_file_bio_num = f["biological_replicates"]
                                if con_bio_num in con_file_bio_num:
                                    con_file_acc = f["accession"]
                                    control_data[con_bio_acc] = con_file_acc

                exp_data = {}
                for e in exp_replicates:
                    exp_bio_acc = e["library"]["biosample"]["accession"]
                    exp_bio_num = e["biological_replicate_number"]
                    for f in obj.get("files", []):
                        if f.get("file_type", "") == "fastq":
                            exp_file_bio_num = f["biological_replicates"]
                            if exp_bio_num in exp_file_bio_num:
                                exp_file_acc = f["accession"]
                                exp_data[exp_bio_acc] = exp_file_acc

                for key in exp_data.keys():
                    if control_data.get(key):
                        temp = {"Experiment": exp_data[key], "Control": control_data[key]}
                        #print(exp_data[key], control_data[key])
                    data.append(temp)
                #print("control_data", control_data)
                #print("exp_data", exp_data)
            else:
                print("ERROR", acc, "has", len(possible_controls), "possible_controls and", len(exp_replicates), "replicates")
        # here we check the two dictionaries
        for e in exp_fastqs.keys():
            for c in con_fastqs.keys():
                if exp_fastqs[e] == con_fastqs[c]:
                    exp = [e, exp_fastqs[e]]
                    con = [c, con_fastqs[c]]
                    temp = {"Experiment": exp, "Control": con}
                    data.append(temp)

    if len(data) > 0:
        print("Experiment_File\tControl_File")
        for d in data:
            print(d["Experiment"], d["Control"])

if __name__ == '__main__':
        main()
