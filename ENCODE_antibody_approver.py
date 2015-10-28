import argparse
import os.path
import csv
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
    parser.add_argument('--user',
                        help="User uuid or @id for updating. Default is ")
    parser.add_argument('--infile',
                        help="TSV with format TBD")
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
    if "users" not in args.user:
        args.user = "/users/" + args.user
    reviewed_by = args.user
    data = []
    idList = []
    with open(args.infile, "r") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            data.append(row)
    for item in data:
        lanes = item.get("lanes", "")
        if "," in lanes:
            # split the item into a list
            lanes = list(set(lanes.split(",")))
        else:
            # just one put it into a list
            lanes = [lanes]
        item["lanes"] = lanes
        if not any(item["notes"]):
            item.pop("notes")
        if item.get("@id") not in idList:
            idList.append(item["@id"])
    objDict = {key: [] for key in idList}
    for item in data:
        objDict.get(item.get("@id", ""), "").append(item)
    print(objDict)

    for idNum in objDict.keys():
        antibody = encodedcc.get_ENCODE(idNum, connection)
        new_antibody = antibody
        if antibody.get("primary_characterization_method"):
            reviews = antibody.get("characterization_reviews", [])
            enc_docs = antibody.get("documents", [])
            file_docs = []
            for obj in objDict[idNum]:
                file_docs.append(obj.get("documents"))
            for r in reviews:
                for line in objDict.get(idNum, ""):
                    for lane in line["lanes"]:
                        if lane == r["lane"]:
                            if line["status"] == "pending dcc review":
                                print("can't set to pending review, need manual override")
                                fin = input("Change the status to 'pending dcc review'? Y/N ")
                                if fin.upper() == "Y":
                                    r["lane_status"] = line["lane_status"]
                                else:
                                    pass
                            else:
                                r["lane_status"] = line["lane_status"]
            # now all lanes in reviews should be updated to document
            # there could be lane in document not in reviews

            if antibody.get("lab", "") == "/labs/michael-snyder/":
                # make sure special document is added if not in the file
                if antibody["primary_characterization_method"] == "immunoprecipitation":
                    if len(reviews) == 1:
                        # fix lane number
                        reviews[0]["lane"] = 3


            enc_lanes = []
            enc_comp = 0
            enc_ncomp = 0
            other = 0
            for r in reviews:
                if r.get("lane"):
                    enc_lanes.append(r["lane"])
                if r.get("lane_status", "") == "compliant":
                    enc_comp = enc_comp + 1
                elif r.get("lane_status", "") == "not compliant":
                    enc_ncomp = enc_ncomp + 1
                else:
                    other = other + 1
            print(enc_lanes)
            if other > 0:
                print("not all lanes have allowed status")
            elif enc_comp > 0:
                print("compliant")
                new_antibody["status"] = "compliant"
            elif other == 0 and enc_comp == 0 and enc_ncomp > 0:
                print("not compliant")
                new_antibody["status"] = "not compliant"

        else:
            print("antibody", antibody.get("uuid"), "is not a primary characterization")

if __name__ == '__main__':
        main()
