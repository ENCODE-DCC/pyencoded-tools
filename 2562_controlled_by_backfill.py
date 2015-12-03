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
    parser.add_argument('--method',
                        help="'single' = there is only one replicate in the control, \
                              'multi' = one control with same number of replicates as experiment has replicates, \
                              'biosample' = multiple controls should be matched on the biosample\
                              default is multi",
                        choices=["single", "multi", "biosample"], default="multi")
    parser.add_argument('--rampage',
                        help="Used for RAMPAGE experiments, ignores value of paired-end. Default is False",
                        default=False,
                        action='store_true')
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


class BackFill:
    def __init__(self, connection, rampage=False, debug=False):
        self.connection = connection
        self.data = []
        self.DEBUG = debug
        self.RAMPAGE = rampage

    def single_rep(self, obj):
        '''one control with one replicate in control,
        multiple replicates in experiment'''
        control_files = obj["possible_controls"][0].get("files")
        control_replicates = obj["possible_controls"][0].get("replicates")
        exp_data = {}
        con_data = {}
        if control_files:
            c = encodedcc.get_ENCODE(control_files[0], self.connection, frame="embedded")
            if c.get("file_type", "") == "fastq":
                con_file_bio_num = c.get("biological_replicates")
                con_file_paired = c.get("paired_end")
                con_file_acc = c["accession"]
                if self.RAMPAGE:
                    con_file_paired = "rampage"
                con_pair = str(con_file_bio_num[0]) + "-" + str(con_file_paired)
                con_data[con_file_acc] = con_pair

            if control_replicates:
                for e in obj["files"]:
                    if e.get("file_type", "") == "fastq":
                        exp_file_bio_num = e.get("biological_replicates")
                        exp_file_paired = e.get("paired_end")
                        exp_file_acc = e["accession"]
                        if self.RAMPAGE:
                            exp_file_paired = "rampage"
                        exp_pair = str(exp_file_bio_num[0]) + "-" + str(exp_file_paired)
                        exp_data[exp_file_acc] = exp_pair

            for e_key in exp_data.keys():
                for c_key in con_data.keys():
                    if con_data[c_key] == exp_data[e_key]:
                        temp = {"Experiment": e_key, "Control": c_key}
                        self.data.append(temp)

            if self.DEBUG:
                print("experiment files", exp_data)
                print("control files", con_data)

    def multi_rep(self, obj):
        '''one control, with one replicate in
        control per replicate in experiment'''
        control_files = obj["possible_controls"][0].get("files", [])
        control_replicates = obj["possible_controls"][0].get("replicates", [])
        exp_data = {}
        con_data = {}
        print(len(obj["replicates"]))
        print(len(control_replicates))
        if control_replicates and (len(obj["replicates"]) == len(control_replicates)):
            print("hihihihih")
            for e in obj["files"]:
                if e.get("file_type", "") == "fastq":
                    exp_file_bio_num = e.get("biological_replicates")
                    exp_file_paired = e.get("paired_end")
                    exp_file_acc = e["accession"]
                    if self.RAMPAGE:
                        exp_file_paired = "rampage"
                    exp_pair = str(exp_file_bio_num[0]) + "-" + str(exp_file_paired)
                    exp_data[exp_file_acc] = exp_pair

            for con in control_files:
                c = encodedcc.get_ENCODE(con, self.connection, frame="embedded")
                if c.get("file_type", "") == "fastq":
                    con_file_bio_num = c.get("biological_replicates")
                    con_file_paired = c.get("paired_end")
                    con_file_acc = c["accession"]
                    if self.RAMPAGE:
                        con_file_paired = "rampage"
                    con_pair = str(con_file_bio_num[0]) + "-" + str(con_file_paired)
                    con_data[con_file_acc] = con_pair

        for e_key in exp_data.keys():
            for c_key in con_data.keys():
                if con_data[c_key] == exp_data[e_key]:
                    temp = {"Experiment": e_key, "Control": c_key}
                    self.data.append(temp)

        if self.DEBUG:
            print("experiment files", exp_data)
            print("control files", con_data)

    def multi_control(self, obj):
        '''multiple controls, match on biosample'''
        con_data = {}
        for c in obj["possible_controls"]:
            con = encodedcc.get_ENCODE(c["accession"], self.connection, frame="embedded")
            for rep in con.get("replicates", []):
                con_bio_acc = rep["library"]["biosample"]["accession"]
                con_bio_num = rep["biological_replicate_number"]
                for fi in c.get("files", []):
                    f = encodedcc.get_ENCODE(fi, self.connection, frame="embedded")
                    if f.get("file_type", "") == "fastq":
                        con_file_bio_num = f["biological_replicates"]
                        if con_bio_num in con_file_bio_num:
                            con_file_acc = f["accession"]
                            con_data[con_bio_acc] = con_file_acc

        exp_data = {}
        for e in obj["replicates"]:
            exp_bio_acc = e["library"]["biosample"]["accession"]
            exp_bio_num = e["biological_replicate_number"]
            for f in obj["files"]:
                if f.get("file_type", "") == "fastq":
                    exp_file_bio_num = f["biological_replicates"]
                    if exp_bio_num in exp_file_bio_num:
                        exp_file_acc = f["accession"]
                        exp_data[exp_bio_acc] = exp_file_acc

        for key in exp_data.keys():
            if con_data.get(key):
                temp = {"Experiment": exp_data[key], "Control": con_data[key]}
                self.data.append(temp)

        if self.DEBUG:
            print("experiment files", exp_data)
            print("control files", con_data)


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
    if len(accessions) == 0:
        print("No accessions to check!")
    else:
        for acc in accessions:
            if args.debug:
                print("Experiment", acc)
            obj = encodedcc.get_ENCODE(acc, connection, frame="embedded")
            isValid = True
            if len(obj.get("possible_controls", [])) == 0:
                if args.debug:
                    print("Missing possible controls for " + acc, file=sys.stderr)
                isValid = False
            if len(obj.get("replicates", [])) == 0:
                if args.debug:
                    print("Missing replicates for " + acc, file=sys.stderr)
                isValid = False
            if len(obj.get("files", [])) == 0:
                if args.debug:
                    print("Missing files for " + acc, file=sys.stderr)
                isValid = False
            if isValid:
                b = BackFill(connection, rampage=args.rampage, debug=args.debug)
                if args.method == "single":
                    b.single_rep(obj)
                elif args.method == "multi":
                    b.multi_rep(obj)
                elif args.method == "biosample":
                    b.multi_control(obj)
                else:
                    print("ERROR: unrecognized method: " + args.method, file=sys.stderr)
                    sys.exit(1)
        if len(b.data) > 0:
            print("Experiment_File\tControl_File")
            for d in b.data:
                print(d["Experiment"] + "\t" + d["Control"])

if __name__ == '__main__':
        main()
