#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os.path
import encodedcc
import csv
from urllib.parse import quote

EPILOG = '''
This script uses the matrix view available at
"https://www.encodeproject.org/matrix/?type=Experiment"
to find and total the Error and Not Compliant audits

This script outputs a TSV file that has been formatted so that when it is
opened in Excel each cell with results will also be a link to the search
page used to generate that cell data

For more details:
        %(prog)s --help

This script will print out the following during it's run:
WARNING:root:No results found
This is due to how the long and short RNA-seq are searched
and it does not affect the final results of the script

all commands need to be quote enclosed

'--rfa' command uses the award.rfa property to refine inital matrix

    Ex: %(prog)s --rfa "ENCODE,Roadmap"


'--species' command uses the organism.name property to refine the inital matrix

    Ex: %(prog)s --species "celegans,human,mouse"


'--lab' command uses the lab.title property to refine inital matrix

    Ex: %(prog)s --lab "bing-ren,j-micheal-cherry"


'--status' uses the status property to refine inital matrix

    Ex: %(prog)s --status "released,submitted"

the usual list of assay this script shows is
    Short RNA-seq, Long RNA-seq, microRNA profiling by array assay, microRNA-seq
    DNase-seq, whole-genome shotgun bisulfite sequencing, RAMPAGE, CAGE

use the '--all' command to select all the available assays for display
the output file can be renamed using the '--outfile' option
the '--allaudits' command will also list the "WARNING" and "DCC ACTION" audits
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
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
    parser.add_argument('--rfa',
                        help="refine search with award.rfa\
                        write as comma separated list\
                        ex: ENCODE,Roadmap")
    parser.add_argument('--species',
                        help="refine search with species using the organism.name property\
                        ex: celegans, human, mouse\
                        write as comma separated list\
                        ex: celegans,human,mouse")
    parser.add_argument('--status',
                        help="refine search with status\
                        write as comma separated list\
                        ex: released,submitted")
    parser.add_argument('--lab',
                        help="refine search with lab.name\
                        write as comma separated list\
                        ex: bing-ren,j-micheal-cherry\
                        lab name format should be Firstname Lastname, Location")
    parser.add_argument('--all',
                        help="use the full list of assays, default is false",
                        default=False,
                        action="store_true")
    parser.add_argument('--outfile',
                        default="Error_Count.tsv",
                        help="name the outfile")
    parser.add_argument('--allaudits',
                        help="show all the audit counts, default is false",
                        default=False,
                        action="store_true")
    args = parser.parse_args()
    return args


def audit_count(facets, total, allaudits):
    error = 0
    not_compliant = 0
    warning = 0
    dcc_action = 0
    if any(facets):
        for f in facets:
            if "ERROR" in f["title"]:
                for t in f["terms"]:
                    if t["doc_count"] > 0:
                        error += t["doc_count"]
            if "NOT COMPLIANT" in f["title"]:
                for t in f["terms"]:
                    if t["doc_count"] > 0:
                        not_compliant += t["doc_count"]
            if allaudits:
                if "WARNING" in f["title"]:
                    for t in f["terms"]:
                        if t["doc_count"] > 0:
                            warning += t["doc_count"]
                if "DCC ACTION" in f["title"]:
                    for t in f["terms"]:
                        if t["doc_count"] > 0:
                            dcc_action += t["doc_count"]
    return total, error, not_compliant, warning, dcc_action


def main():
    print("This script outputs a 'No Results Found' error.")
    print("This is due to the Long/Short RNA-seq, it does not affect the final results")
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    search_string = "/matrix/?type=Experiment"
    rfa_string = ""
    species_string = ""
    status_string = ""
    lab_string = ""
    if args.rfa:
        rfa_list = args.rfa.split(",")
        for r in rfa_list:
            rfa_string += "&award.rfa=" + r
    if args.species:
        species_list = args.species.split(",")
        for r in species_list:
            species_string += "&replicates.library.biosample.donor.organism.name=" + r
    if args.status:
        status_list = args.status.split(",")
        for r in status_list:
            status_string += "&status=" + r
    if args.lab:
        lab_list = args.lab.split(",")
        for r in lab_list:
            lab_string += "&lab.name=" + r
    full_string = rfa_string + species_string + status_string + lab_string
    search_string += full_string
    matrix_url = '=HYPERLINK("{}","{}")'.format(connection.server + search_string, connection.server + search_string)

    matrix = encodedcc.get_ENCODE(search_string, connection).get("matrix")
    x_values = matrix.get("x")
    y_values = matrix.get("y")

    y_buckets = y_values["replicates.library.biosample.biosample_type"].get("buckets")
    x_buckets = x_values.get("buckets")
    if args.all:
        full_list = []
        for x in x_buckets:
            full_list.append(x["key"])
    else:
        full_list = ["RNA-seq", "microRNA profiling by array assay", "microRNA-seq", "DNase-seq", "whole-genome shotgun bisulfite sequencing", "RAMPAGE", "CAGE"]
    temp_list = list(full_list)
    if "RNA-seq" in temp_list:
        temp_list.remove("RNA-seq")
    headers = [matrix_url] + ["Long RNA-seq", "Short RNA-seq"] + temp_list + ["TOTAL"]

    final_assay_search = ""  # this will be used to total rows
    for name in full_list:
        final_assay_search += "&assay_term_name=" + name
    final_bio_search = ""  # this will be used to total columns

    col_dict = dict.fromkeys(headers)
    for k in col_dict.keys():
        col_dict[k] = []
    with open(args.outfile, "w") as tsvfile:
        dictwriter = csv.DictWriter(tsvfile, delimiter="\t", fieldnames=headers)
        dictwriter.writeheader()
        for y in y_buckets:
            inner_buckets = y["biosample_term_name"].get("buckets")
            group_dict = dict.fromkeys(headers)
            group_dict[matrix_url] = y["key"]
            dictwriter.writerow(group_dict)
            for item in inner_buckets:
                bio_name = item["key"]
                final_bio_search += "&biosample_term_name=" + quote(bio_name)
                assay_list = item["assay_term_name"]
                row_dict = dict.fromkeys(headers)
                for k in row_dict.keys():
                    row_dict[k] = 0
                row_dict[matrix_url] = bio_name
                row_count = []
                for x in range(len(assay_list)):
                    assay_name = x_buckets[x]["key"]
                    if assay_name in full_list:
                        if assay_list[x] > 0:
                            search = "/search/?type=Experiment&biosample_term_name=" + quote(bio_name) + "&assay_term_name=" + assay_name + full_string
                            if assay_name == "RNA-seq":
                                short_search = search + "&replicates.library.size_range=<200"
                                long_search = search + "&replicates.library.size_range!=<200"

                                short_url = connection.server + short_search
                                long_url = connection.server + long_search

                                short_facets = encodedcc.get_ENCODE(short_search, connection)
                                long_facets = encodedcc.get_ENCODE(long_search, connection)

                                if short_facets.get("total") == 0:
                                    row_dict["Short RNA-seq"] = 0
                                    row_count.append([0, 0, 0, 0, 0])
                                    col_dict["Short RNA-seq"].append([0, 0, 0, 0, 0])
                                else:
                                    total, error, not_compliant, warning, dcc_action = audit_count(short_facets.get("facets", []), short_facets.get("total"), args.allaudits)
                                    if args.allaudits:
                                        string = '=HYPERLINK("{}","{}, {}E, {}NC, {}W, {}DCC")'.format(short_url, total, error, not_compliant, warning, dcc_action)
                                    else:
                                        string = '=HYPERLINK("{}","{}, {}E, {}NC")'.format(short_url, total, error, not_compliant)
                                    row_dict["Short RNA-seq"] = string
                                    row_count.append([total, error, not_compliant, warning, dcc_action])
                                    col_dict["Short RNA-seq"].append([total, error, not_compliant, warning, dcc_action])

                                if long_facets.get("total") == 0:
                                    row_dict["Long RNA-seq"] = 0
                                    row_count.append([0, 0, 0, 0, 0])
                                    col_dict["Long RNA-seq"].append([0, 0, 0, 0, 0])
                                else:
                                    total, error, not_compliant, warning, dcc_action = audit_count(long_facets.get("facets", []), long_facets.get("total"), args.allaudits)
                                    if args.allaudits:
                                        string = '=HYPERLINK("{}","{}, {}E, {}NC, {}W, {}DCC")'.format(long_url, total, error, not_compliant, warning, dcc_action)
                                    else:
                                        string = '=HYPERLINK("{}","{}, {}E, {}NC")'.format(long_url, total, error, not_compliant)
                                    row_dict["Long RNA-seq"] = string
                                    row_count.append([total, error, not_compliant, warning, dcc_action])
                                    col_dict["Long RNA-seq"].append([total, error, not_compliant, warning, dcc_action])
                            else:
                                url = connection.server + search
                                facets = encodedcc.get_ENCODE(search, connection).get("facets", [])
                                total, error, not_compliant, warning, dcc_action = audit_count(facets, assay_list[x], args.allaudits)
                                if args.allaudits:
                                    string = '=HYPERLINK("{}","{}, {}E, {}NC, {}W, {}DCC")'.format(url, total, error, not_compliant, warning, dcc_action)
                                else:
                                    string = '=HYPERLINK("{}","{}, {}E, {}NC")'.format(url, total, error, not_compliant)
                                row_count.append([total, error, not_compliant, warning, dcc_action])
                                row_dict[assay_name] = string
                                col_dict[assay_name].append([total, error, not_compliant, warning, dcc_action])
                        else:
                            if assay_name == "RNA-seq":
                                row_dict["Short RNA-seq"] = 0
                                row_dict["Long RNA-seq"] = 0
                                col_dict["Short RNA-seq"].append([0, 0, 0, 0, 0])
                                col_dict["Long RNA-seq"].append([0, 0, 0, 0, 0])
                            else:
                                row_dict[assay_name] = 0
                                col_dict[assay_name].append([0, 0, 0, 0, 0])
                            row_count.append([0, 0, 0, 0, 0])

                row_total = 0
                row_error = 0
                row_not_compliant = 0
                row_warning = 0
                row_dcc_action = 0
                bio_total = "/search/?type=Experiment&biosample_term_name=" + quote(bio_name) + final_assay_search + full_string
                bio_url = connection.server + bio_total
                for col in row_count:
                        row_total += col[0]
                        row_error += col[1]
                        row_not_compliant += col[2]
                        row_warning += col[3]
                        row_dcc_action += col[4]
                if args.allaudits:
                    row_dict["TOTAL"] = '=HYPERLINK("{}", "{}, {}E, {}NC, {}W, {}DCC")'.format(bio_url, row_total, row_error, row_not_compliant, row_warning, row_dcc_action)
                else:
                    row_dict["TOTAL"] = '=HYPERLINK("{}", "{}, {}E, {}NC")'.format(bio_url, row_total, row_error, row_not_compliant)
                dictwriter.writerow(row_dict)
        total = 0
        error = 0
        not_compliant = 0
        warning = 0
        dcc_action = 0
        total_dict = dict.fromkeys(headers)
        total_dict[matrix_url] = "TOTALS"
        for key in col_dict.keys():
            if key in headers and key != matrix_url:
                col_total = 0
                col_error = 0
                col_not_compliant = 0
                col_warning = 0
                col_dcc_action = 0
                for cell in col_dict[key]:
                    col_total += cell[0]
                    col_error += cell[1]
                    col_not_compliant += cell[2]
                    col_warning += cell[3]
                    col_dcc_action += cell[4]
                total += col_total
                error += col_error
                not_compliant += col_not_compliant
                warning += col_warning
                dcc_action += col_dcc_action
                if key == "Long RNA-seq":
                    assay_total = "/search/?type=Experiment&assay_term_name=RNA-seq&replicates.library.size_range!=<200" + final_bio_search + full_string
                elif key == "Short RNA-seq":
                    assay_total = "/search/?type=Experiment&assay_term_name=RNA-seq&replicates.library.size_range=<200" + final_bio_search + full_string
                else:
                    assay_total = "/search/?type=Experiment&assay_term_name=" + key + final_bio_search + full_string
                assay_url = connection.server + assay_total
                if args.allaudits:
                    total_dict[key] = '=HYPERLINK("{}", "{}, {}E, {}NC, {}W, {}DCC")'.format(assay_url, col_total, col_error, col_not_compliant, col_warning, col_dcc_action)
                else:
                    total_dict[key] = '=HYPERLINK("{}", "{}, {}E, {}NC")'.format(assay_url, col_total, col_error, col_not_compliant)
        full_search = "/search/?type=Experiment" + final_assay_search + final_bio_search + full_string
        full_url = connection.server + full_search
        if args.allaudits:
            total_dict["TOTAL"] = '=HYPERLINK("{}", "{}, {}E, {}NC, {}W, {}DCC")'.format(full_url, total, error, not_compliant, warning, dcc_action)
        else:
            total_dict["TOTAL"] = '=HYPERLINK("{}", "{}, {}E, {}NC")'.format(full_url, total, error, not_compliant)
        dictwriter.writerow(total_dict)

    print("Output saved to {}, open this file with Google Docs Sheets, don't use Excel because it sucks".format(args.outfile))


if __name__ == '__main__':
        main()
