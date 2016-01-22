import argparse
import os.path
import encodedcc
import sys
import csv
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
                        write as semicolon separated list\
                        ex: 'ENCODE;Roadmap'")
    parser.add_argument('--species',
                        help="refine search with species using the organism.name property\
                        ex: celegans, human, mouse\
                        write as semicolon separated list\
                        ex: 'celegans;human;mouse'")
    parser.add_argument('--status',
                        help="refine search with status\
                        write as semicolon separated list\
                        ex: 'released;submitted'")
    parser.add_argument('--lab',
                        help="refine search with lab title\
                        write as quote enclosed semicolon separated list\
                        ex: \"Bing Ren, UCSD;J. Micheal Cherry, Stanford\"\
                        lab name format should be Firstname Lastname, Location")
    args = parser.parse_args()
    return args


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    search_string = "/matrix/?type=Experiment"
    if args.rfa:
        rfa_list = args.rfa.split(";")
        ministring = ""
        for r in rfa_list:
            ministring += "&award.project=" + r
        search_string += ministring  # ENCODE
    if args.species:
        species_list = args.species.split(";")
        ministring = ""
        for r in species_list:
            ministring += "&replicates.library.biosample.donor.organism.name=" + r
        search_string += ministring  # celegans
    if args.status:
        status_list = args.status.split(";")
        ministring = ""
        for r in status_list:
            ministring += "&status=" + r
        search_string += ministring
    if args.lab:
        lab_list = args.lab.split(";")
        ministring = ""
        for r in lab_list:
            r = r.replace(" ", "+")
            ministring += "&lab.title=" + r
        search_string += ministring  # Bing+Ren,+UCSD

    matrix = encodedcc.get_ENCODE(search_string, connection).get("matrix")
    x_values = matrix.get("x")
    y_values = matrix.get("y")

    y_buckets = y_values["replicates.library.biosample.biosample_type"].get("buckets")
    x_buckets = x_values.get("buckets")

    def audit_count(facets, url):
        error = 0
        not_compliant = 0
        if any(facets):
            for f in facets:
                if "ERROR" in f["title"]:
                    for t in f["terms"]:
                        if t["doc_count"] > 0:
                            error += t["doc_count"]
                elif "NOT COMPLIANT" in f["title"]:
                    for t in f["terms"]:
                        if t["doc_count"] > 0:
                            not_compliant += t["doc_count"]
        string = '=HYPERLINK("{}","{}, {}E, {}NC")'.format(url, assay_list[x], error, not_compliant)
        #temp = {"Total": assay_list[x], "Error": error, "NotCompliant": not_compliant, "URL": url}
        return string

    cricket_list = ["RNA-seq", "microRNA profiling by array assay", "microRNA-seq", "DNase-seq", "whole-genome shotgun bisulfite sequencing", "RAMPAGE", "CAGE"]
    temp_list = list(cricket_list)
    temp_list.remove("RNA-seq")
    headers = [""] + ["Long RNA-seq", "Short RNA-seq"] + temp_list
    with open("Experiment_tsv.txt", "w") as tsvfile:
        dictwriter = csv.DictWriter(tsvfile, delimiter="\t", fieldnames=headers)
        dictwriter.writeheader()
        for y in y_buckets:
            inner_buckets = y["biosample_term_name"].get("buckets")
            group_dict = dict.fromkeys(headers)
            group_dict[""] = y["key"]
            dictwriter.writerow(group_dict)
            for item in inner_buckets:
                bio_name = item["key"]
                assay_list = item["assay_term_name"]
                #here bio_name got written
                #initialize dictionary row here as this is the fresh row
                row_dict = dict.fromkeys(headers)
                row_dict[""] = bio_name

                for x in range(len(assay_list)):
                    assay_name = x_buckets[x]["key"]
                    if assay_name in cricket_list:
                        if assay_list[x] > 0:
                            search = "/search/?type=Experiment&biosample_term_name=" + quote(bio_name) + "&assay_term_name=" + assay_name
                            if assay_name == "RNA-seq":
                                rshort = "&replicates.library.size_range=<200"
                                rlong = "&replicates.library.size_range!=<200"
                                short_search = search + rshort
                                long_search = search + rlong

                                short_url = connection.server + short_search
                                long_url = connection.server + long_search

                                short_facets = encodedcc.get_ENCODE(short_search, connection).get("facets", [])
                                long_facets = encodedcc.get_ENCODE(long_search, connection).get("facets", [])

                                s = audit_count(short_facets, short_url)
                                l = audit_count(long_facets, long_url)

                                row_dict["Short RNA-seq"] = s
                                row_dict["Long RNA-seq"] = l
                            else:
                                url = connection.server + search
                                facets = encodedcc.get_ENCODE(search, connection).get("facets", [])
                                temp = audit_count(facets, url)
                                row_dict[assay_name] = temp
                        else:
                            if assay_name == "RNA-seq":
                                row_dict["Short RNA-seq"] = 0
                                row_dict["Long RNA-seq"] = 0
                            else:
                                row_dict[assay_name] = 0
                dictwriter.writerow(row_dict)

    print("done")


if __name__ == '__main__':
        main()
