#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os.path
import encodedcc
import sys

EPILOG = '''
Script to fix the controlled_by backfill problems
This is a dryrun default script, run with '--update' to PATCH data

Useage:

    %(prog)s --infile MyFile.txt
    %(prog)s --infile ENCSR000AAA
    %(prog)s --infile ENCSR000AAA,ENCSR000AAB,ENCSR000AAC
    %(prog)s --query "/search/?type=Experiment"

Script will take a file with single column list of accessions
Can also take a single accession or comma separated list of accessions
A query from which to gather accessions


    %(prog)s --method single
    %(prog)s --method multi
    %(prog)s --method biosample

There are three methods to pick from
"single" assumes one replicate in the control
"multi" assumes one control with number of replicates equal to number of replicates in experiment
"biosample" assumes multiple controls that should be matched on biosample

By not selecting the '--method' option the script will try to guess at what the type is


    %(prog)s --ignore_runtype

This makes the script ignore the value of the paired ends, default is off


    %(prog)s --missing

Script will print out only the names of files missing controlled_by


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
                              'biosample' = multiple controls should be matched on the biosample",
                        choices=["single", "multi", "biosample"])
    parser.add_argument('--ignore_runtype',
                        help="Ignores value of paired-end. Default is off",
                        default=False,
                        action='store_true')
    parser.add_argument('--infile',
                        help="file containing single column list of object accessions,\
                        single accession, or comma separated list of accessions")
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
                        help="Print debug messages.  Default is off.")
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH the data.  Default is off")
    parser.add_argument('--missing',
                        default=False,
                        action='store_true',
                        help="Only print files that are missing controlled_by.\
                        Default is off")
    args = parser.parse_args()
    return args


class BackFill:
    def __init__(self, connection, dataList, debug=False, missing=False, update=False):
        self.connection = connection
        self.data = dataList
        self.DEBUG = debug
        self.MISSING = missing
        self.update = update

    def single_rep(self, obj):
        '''one control with one replicate in control,
        multiple replicates in experiment'''
        control_files = encodedcc.get_ENCODE(obj["possible_controls"][0]["accession"], self.connection, frame="embedded").get("files", [])
        if len(control_files) == 0:
            if self.DEBUG:
                print("Control object {} has no files".format(obj["possible_controls"][0]["accession"]), file=sys.stderr)
            return
        for c in control_files:
            if c.get("file_type", "") == "fastq":
                exp_list = []
                for e in obj["files"]:
                    if e.get("file_type", "") == "fastq":
                        if not self.MISSING or (self.MISSING and not e.get("controlled_by")):
                                exp_list.append(e["accession"])
                if self.update:
                    for exp in exp_list:
                        temp = encodedcc.get_ENCODE(exp, self.connection).get("controlled_by", [])
                        control = temp.append(c["accession"])
                        patch_dict = {"controlled_by": control}
                        print("PATCHing experiment file {} with controlled_by {}".format(exp, c["accession"]))
                        encodedcc.patch_ENCODE(exp, self.connection, patch_dict)
                temp = {"Exp Accession": obj["accession"], "Check type": "Single", "Experiment": exp_list, "Control": c["accession"]}
                if len(exp_list) > 0:
                    self.data.append(temp)
                if self.DEBUG:
                    print("experiment files {}".format(temp["Experiment"]))
                    print("control files {}".format(temp["Control"]))

    def multi_rep(self, obj, ignore_runtype=False):
        '''one control, with one replicate in
        control per replicate in experiment'''
        control_files = encodedcc.get_ENCODE(obj["possible_controls"][0]["accession"], self.connection, frame="embedded").get("files", [])
        control_replicates = obj["possible_controls"][0].get("replicates", [])
        exp_data = {}
        con_data = {}
        if len(control_replicates) != len(obj["replicates"]):
            if self.DEBUG:
                print("Control has {} replicates and experiment has {} replicates".format(len(control_replicates), len(obj["replicates"])), file=sys.stderr)
            return
        if len(control_files) == 0:
            if self.DEBUG:
                print("Control {} has no files".format(obj["possible_controls"][0]["accession"]), file=sys.stderr)
            return
        for e in obj["files"]:
            if e.get("file_type", "") == "fastq":
                if not self.MISSING or (self.MISSING and not e.get("controlled_by")):
                    exp_file_bio_num = e.get("biological_replicates")
                    exp_file_paired = e.get("paired_end")
                    exp_file_acc = e["accession"]
                    if ignore_runtype:
                        exp_file_paired = None
                    exp_pair = str(exp_file_bio_num[0]) + "-" + str(exp_file_paired)
                    exp_data[exp_file_acc] = exp_pair

        for c in control_files:
            if c.get("file_type", "") == "fastq":
                con_file_bio_num = c.get("biological_replicates")
                con_file_paired = c.get("paired_end")
                con_file_acc = c["accession"]
                if ignore_runtype:
                    con_file_paired = None
                con_pair = str(con_file_bio_num[0]) + "-" + str(con_file_paired)
                con_data[con_file_acc] = con_pair

        if ignore_runtype:
            for e_key in exp_data.keys():
                con_list = []
                for c_key in con_data.keys():
                    if exp_data[e_key] == con_data[c_key]:
                        con_list.append(c_key)
                if self.update:
                    for con in con_list:
                        temp = encodedcc.get_ENCODE(exp, self.connection).get("controlled_by", [])
                        control = temp.append(con)
                        patch_dict = {"controlled_by": control}
                        print("patching experiment file {} with controlled_by {}".format(e_key, con))
                        encodedcc.patch_ENCODE(e_key, self.connection, patch_dict)
                temp = {"Exp Accession": obj["accession"], "Check type": "Multi-runtype ignored", "Experiment": e_key, "Control": con_list}
                self.data.append(temp)
                if self.DEBUG:
                    print("experiment files", e_key)
                    print("control files", con_list)
        else:
            for c_key in con_data.keys():
                exp_list = []
                for e_key in exp_data.keys():
                    if con_data[c_key] == exp_data[e_key]:
                        exp_list.append(e_key)
                if self.update:
                    for exp in exp_list:
                        temp = encodedcc.get_ENCODE(exp, self.connection).get("controlled_by", [])
                        control = temp.append(c_key)
                        patch_dict = {"controlled_by": control}
                        print("patching experiment file {} with controlled_by {}".format(exp, c_key))
                        encodedcc.patch_ENCODE(exp, self.connection, patch_dict)
                temp = {"Exp Accession": obj["accession"], "Check type": "Multi", "Experiment": exp_list, "Control": c_key}
                if len(exp_list) > 0:
                    self.data.append(temp)
                if self.DEBUG:
                    print("experiment files", exp_list)
                    print("control files", c_key)

    def multi_control(self, obj):
        '''multiple controls, match on biosample'''
        con_data = {}
        val = True
        for con in obj["possible_controls"]:
            c = encodedcc.get_ENCODE(con["accession"], self.connection, frame="embedded")
            if c.get("replicates"):
                for rep in c["replicates"]:
                    if c.get("files"):
                        con_bio_acc = rep["library"]["biosample"]["accession"]
                        con_bio_num = rep["biological_replicate_number"]
                        for f in c["files"]:
                            if f.get("file_type", "") == "fastq":
                                con_file_bio_num = f["biological_replicates"]
                                if con_bio_num in con_file_bio_num:
                                    con_file_acc = f["accession"]
                                    con_data[con_bio_acc] = con_file_acc
                    else:
                        if self.DEBUG:
                            print("No files found for control {}".format(con["accession"]), file=sys.stderr)
                        val = False
            else:
                if self.DEBUG:
                    print("No replicates found in control {}".format(con["accession"]), file=sys.stderr)
                val = False

        if val:
            exp_data = {}
            for e in obj["replicates"]:
                exp_bio_acc = e["library"]["biosample"]["accession"]
                exp_bio_num = e["biological_replicate_number"]
                for f in obj["files"]:
                    if f.get("file_type", "") == "fastq":
                        if not self.MISSING or (self.MISSING and not f.get("controlled_by")):
                            exp_file_bio_num = f["biological_replicates"]
                            if exp_bio_num in exp_file_bio_num:
                                exp_file_acc = f["accession"]
                                exp_data[exp_bio_acc] = exp_file_acc

            for key in exp_data.keys():
                if con_data.get(key):
                    if args.update:
                        temp = encodedcc.get_ENCODE(exp, self.connection).get("controlled_by", [])
                        control = temp.append(con_data[key])
                        patch_dict = {"controlled_by": control}
                        print("patching experiment file {} with controlled_by {}".format(exp_data[key], con_data[key]))
                        encodedcc.patch_ENCODE(exp_data[key], self.connection, patch_dict)
                    temp = {"Exp Accession": obj["accession"], "Check type": "Biosample", "Experiment": exp_data[key], "Control": con_data[key]}
                    self.data.append(temp)
                    if self.DEBUG:
                        print("Biosample {}: files {}".format(key, temp))


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    accessions = []
    if args.update:
        print("This is an UPDATE run data will be PATCHed")
    else:
        print("This is a dryrun, no data will be changed")
    if args.infile:
        if os.path.isfile(args.infile):
            accessions = [line.rstrip('\n') for line in open(args.infile)]
        else:
            accessions = args.infile.split(",")
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
    if len(accessions) == 0:
        # if something happens and we end up with no accessions stop
        print("ERROR: object has no identifier", file=sys.stderr)
        sys.exit(1)
    else:
        dataList = []
        for acc in accessions:
            obj = encodedcc.get_ENCODE(acc, connection, frame="embedded")
            isValid = True
            check = ["replicates", "files"]
            for c in check:
                if not obj.get(c):
                    if args.debug:
                        print("Missing {} for {}".format(c, acc), file=sys.stderr)
                    isValid = False
            if obj.get("possible_controls"):
                for p in obj["possible_controls"]:
                    for c in check:
                        if not obj.get(c):
                            if args.debug:
                                print("Missing {} for {}".format(c, p["accession"]), file=sys.stderr)
                            isValid = False
            else:
                isValid = False
                if args.debug:
                    print("Missing possible_controls for {}".format(acc), file=sys.stderr)
            if isValid:
                b = BackFill(connection, dataList, debug=args.debug, missing=args.missing, update=args.update)
                if args.method == "single":
                    b.single_rep(obj)
                    if args.debug:
                        print("SINGLE REP {}".format(acc))
                elif args.method == "multi":
                    b.multi_rep(obj, args.ignore_runtype)
                    if args.debug:
                        print("MULTI REP {}".format(acc))
                elif args.method == "biosample":
                    b.multi_control(obj)
                    if args.debug:
                        print("BIOSAMPLE {}".format(acc))
                else:
                    exp_rep = len(obj["replicates"])
                    exp_con = len(obj["possible_controls"])
                    if exp_con == 1:
                        # one possible control
                        con_rep = len(obj["possible_controls"][0]["replicates"])
                        if con_rep == exp_rep:
                            # same number experiment replicates as control replicates
                            # method is multi
                            b.multi_rep(obj, args.ignore_runtype)
                            if args.debug:
                                print("MULTI REP {}".format(acc))
                        elif con_rep == 1:
                            # one control replicate and multiple experiment replicates
                            # method is single
                            b.single_rep(obj)
                            if args.debug:
                                print("SINGLE REP {}".format(acc))
                        else:
                            if args.debug:
                                print("Experiment {} contains {} experiment replicates and {} control replicates and so does not fit the current pattern!".format(acc, exp_rep, con_rep))
                    elif exp_con > 1:
                        # more than one possible control
                        con_reps = 0
                        for con in obj["possible_controls"]:
                            if len(con["replicates"]) == 1:
                                con_reps += 1
                        if con_reps == exp_rep:
                            # same number of controls with one replicate as number of experiment replicates
                            # method is biosample
                            b.multi_control(obj)
                            if args.debug:
                                print("BIOSAMPLE {}".format(acc))
                        else:
                            if args.debug:
                                print("Experiment {} contains {} experiment replicates and {} control replicates between {} total controls and so does not fit the current pattern!".format(acc, exp_rep, con_rep, exp_con))
                    else:
                        if args.debug:
                            print("Experiment {} does not fit any of the current patterns!".format(acc))

        if len(dataList) > 0:
            print("Experiment Accession\tCheck Type\tControl Files\tExperiment Files")
            for d in dataList:
                print("{}\t{}\t{}\t{}".format(d["Exp Accession"], d["Check type"], d["Control"], d["Experiment"]))

if __name__ == '__main__':
        main()
