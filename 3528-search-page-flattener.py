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

    parser.add_argument('facet',
                        help="name of facet")
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
    profiles = encodedcc.get_ENCODE("/profiles/", connection).keys()
    if args.facet not in profiles:
        print("Facet must be one of valid options in 'https://www.encodeproject.org/profiles/'")
        sys.exit(1)
    temp = encodedcc.get_ENCODE("/search/?type=" + args.facet, connection)
    facet_list = temp.get("facets", [])
    facet_map = {}
    fields = []
    accessions = []
    for f in facet_list:
        facet_map[f["title"]] = f["field"]
        fields.append(f["field"])
    graph = temp.get("@graph", [])
    for obj in graph[:10]:
        if obj.get("accession"):
            accessions.append(obj["accession"])
        else:
            accessions.append(obj["uuid"])
    accessions = ["ENCSR087PLZ"]
    output = encodedcc.GetFields(connection, facet=[accessions, fields])
    output.get_fields(args)
    data_list = []
    headers = ["Identifier"] + list(facet_map.keys())
    facet_map["Identifier"] = "accession"  # add the identifier to the map
    for d in output.data:
        temp = {}
        for key in facet_map.keys():
            if d.get(facet_map[key]):
                temp[key] = d[facet_map[key]]
        data_list.append(temp)

    writer = csv.DictWriter(sys.stdout, delimiter='\t', fieldnames=headers)
    writer.writeheader()
    for d in data_list:
        writer.writerow(d)


if __name__ == '__main__':
        main()
