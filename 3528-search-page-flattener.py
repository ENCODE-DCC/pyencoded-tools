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
    facet = args.facet
    temp = encodedcc.get_ENCODE("/search/?type=" + facet, connection)
    facet_list = temp.get("facets", [])
    facet_map = {}
    fields = []
    accessions = []
    for f in facet_list:
        facet_map[f["title"]] = f["field"]
        fields.append(f["field"])
    graph = temp.get("@graph", [])
    for obj in graph[:1]:
        if obj.get("accession"):
            accessions.append(obj["accession"])
        else:
            accessions.append(obj["uuid"])
    accessions = ["ENCSR087PLZ"]
    fields = ["replicates.library.biosample.donor.organism.scientific_name", "assay_term_name"]
    output = encodedcc.GetFields(connection, facet=[accessions, fields])
    output.get_fields(args)
    print(output.data)
    facet_dict = {}
    facet_map["Identifier"] = "accession"  # add the identifier to the map
    print(facet_map)
    for d in output.data:
        for key in facet_map.keys():
            if d.get(facet_map[key]):
                print(key, facet_map[key])
                print("yay!")
            #facet_dict[key] = d[facet_map[key]]
    #print(facet_dict)


'''d1 = {"Data Type": "type"}
d2 = [{"type": "value", "id": "num"}, {"type": "value", "id", "num"}]
d3 = {}
for d in d2:
    for key in d1.keys():
        d3[key] = d[d1[key]]

for key in d1:
    for d in d2:
        d1[key] = d2[d1[key]]
d1["Data Type"] = "value"'''


if __name__ == '__main__':
        main()
