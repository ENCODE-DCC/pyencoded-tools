import argparse
import os.path
import encodedcc
import sys
import xlsxwriter

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
    args = parser.parse_args()
    return args


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    matrix = encodedcc.get_ENCODE("/matrix/?type=Experiment", connection).get("matrix")
    x_values = matrix.get("x")
    y_values = matrix.get("y")

    y_buckets = y_values["replicates.library.biosample.biosample_type"].get("buckets")
    x_buckets = x_values.get("buckets")

    workbook = xlsxwriter.Workbook("Experiment_Matrix.xlsx")
    worksheet = workbook.add_worksheet()
    row = 0
    col = 0

    for x in x_buckets:
        worksheet.write(row, col + 1, x["key"])
        col += 1
    col = 0
    row += 1

    url_format = workbook.add_format({
        'font_color': 'blue',
        'underline': 1
        })

    for y in y_buckets[:1]:
        inner_buckets = y["biosample_term_name"].get("buckets")
        for item in inner_buckets[:1]:
            bio_name = item["key"]
            assay_list = item["assay_term_name"]
            col = 0
            for x in range(len(assay_list)):
                assay_name = x_buckets[x]["key"]
                worksheet.write(row, 0, bio_name)
                if assay_list[x] > 0:
                    search = "/search/?type=Experiment&biosample_term_name=" + bio_name + "&assay_term_name=" + assay_name
                    url = connection.server + search
                    facets = encodedcc.get_ENCODE(search, connection).get("facets")
                    error = 0
                    not_compliant = 0
                    for f in facets:
                        if "ERROR" in f["title"]:
                            for t in f["terms"]:
                                if t["doc_count"] > 0:
                                    error += t["doc_count"]
                        elif "NOT COMPLIANT" in f["title"]:
                            for t in f["terms"]:
                                if t["doc_count"] > 0:
                                    not_compliant += t["doc_count"]
                    #print("biosample {}, assay {}, {} Total, {} ERROR, {} NOT COMPLIANT".format(bio_name, assay_name, assay_list[x], error, not_compliant))
                    temp = {"Total": assay_list[x], "Error": error, "NotCompliant": not_compliant, "URL": url}
                    string = "{}, {}E, {}NC".format(assay_list[x], error, not_compliant)
                    worksheet.write_url(row, col + 1, url, url_format, string)
                    col += 1
                    assay_name_errors = {assay_name: temp}
                else:
                    assay_name_errors = {assay_name: 0}
                    worksheet.write(row, col + 1, 0)
                    col += 1
                row += 1

    workbook.close()
    print("done")


if __name__ == '__main__':
        main()
