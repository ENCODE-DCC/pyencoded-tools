import argparse
import os.path
import encodedcc
import sys
import csv

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('search',
                        help="the search query to use")
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
    temp = encodedcc.get_ENCODE(args.search, connection)
    facet_list = temp.get("facets", [])
    graph = temp.get("@graph", [])
    facet_map = {}
    fields = []
    accessions = []
    headers = ["Identifier"]
    for f in facet_list:
        if "audit" in f["field"]:
            pass
        elif f["title"] == f["field"]:
            pass
        else:
            facet_map[f["title"]] = f["field"]
            fields.append(f["field"])
            headers.append(f["title"])
    for obj in graph:
        if obj.get("accession"):
            accessions.append(obj["accession"])
        else:
            accessions.append(obj["uuid"])
    output = encodedcc.GetFields(connection, facet=[accessions, fields])
    output.get_fields(args)
    data_list = []
    facet_map["Identifier"] = "accession"  # add the identifier to the map
    for d in output.data:
        temp = {}
        for key in facet_map.keys():
            if d.get(facet_map[key]):
                temp[key] = d[facet_map[key]]
        data_list.append(temp)
    # removing empty columns from the data
    # check a header value in each dataset, if it appears then keep that header
    # if that header value is empty in all dataset remove header value
    empty_columns = []
    for h in headers:
        count = 0
        for d in data_list:
            if d.get(h) is not None:
                count += 1
        if count == 0:
            empty_columns.append(h)
    fieldnames = [x for x in headers if x not in empty_columns]

    writer = csv.DictWriter(sys.stdout, delimiter='\t', fieldnames=fieldnames)
    writer.writeheader()
    for d in data_list:
        writer.writerow(d)


if __name__ == '__main__':
        main()
