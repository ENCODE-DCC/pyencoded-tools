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
    objList = []  # holds list of all items
    with open(args.infile, "r") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            data.append(row)
    for item in data:
        if item.get("@id") not in objList:
            objList.append(item["@id"])

    compiled_data = []
    for obj in objList:
        for item in data:
            temp = {}
            if item.get("@id") == obj:
                temp["@id"] = item["@id"]
                lanes = item.get("lanes", "")
                if "," in lanes:
                    # split the item into a list
                    lanes = list(set(lanes.split(",")))
                else:
                    # just one put it into a list
                    lanes = [lanes]
                temp["status"] = temp["status"].append([lanes, item.get("lane_status", "")])
                temp["notes"] = temp["notes"].append(item.get("notes", ""))
                temp["documents"] = temp["documents"].append(item.get("documents", ""))
            compiled_data.append(temp)

    '''temp = {"@id": "/antibody-characterizations/123_456/",
                        "status": [
                                    [[1, 2, 3], "compliant"],
                                    [[4, 5, 6], "not compliant"]
                                   ],
                        "notes": ["notes notes", "notes notes"],
                        "documents": ["document", "document"]
                        }'''

    for obj in compiled_data:
        # each of these is the file data for the antibody
        antibody = encodedcc.get_ENCODE(obj, connection)
        # get the antibody, this we will edit and PATCH
        reviews = antibody.get("characterization_reviews", [])
        # get the reviews, this can be checked and edited as needed
        review_nums = []
        file_nums = []
        for r in reviews:
            review_nums.append(r.get("lane"))
        for lane in obj["status"]:
            # cycle through the possible status lists
            for num in lane[0]:
                for val in num:
                    file_nums.append(val)
        # make sure no lanes listed in file that are not in object
        # no lanes listed more than once
        # no lanes are missing
        '''WAIT WAIT WAIT! I don't think I'm checking properly for duplicates'''
        if len(review_nums) > len(file_nums):
            diff_nums = [x for x in review_nums if x not in file_nums]
        else:
            diff_nums = [x for x in file_nums if x not in review_nums]
        if not diff_nums:
            pass





'''
    for idNum in objList:
        antibody = encodedcc.get_ENCODE(idNum, connection, frame="edit")
        if antibody.get("primary_characterization_method"):
            # get the list of reviews from the antibody, can compare them later
            reviews = antibody.get("characterization_reviews", [])
            # get list of documents from ENCODE
            documents = antibody.get("documents", [])
            for item in data:
                if item.get("@id", "'") == idNum:
                    lanes = item.get("lanes", "")
                    if "," in lanes:
                        # split the item into a list
                        lanes = lanes.split(",")
                    else:
                        # just one put it into a list
                        lanes = [lanes]
                    for lane in lanes:
                        # look at lanes and compare to lanes from antibody
                        # check that lane and status match
                        for r in reviews:
                            if r.get("lane") == lane:
                                # this means that the lane number in charactarization_reviews
                                # matches the lane number in the file, check the status
                                r["lane_status"] = item["status"]
                                # remove a lane if it is in the database already
                                lanes.pop(lane)
                         temp = {"lane": lane, "lane_status": item.get("lane_status", "")}
                         reviews.append(temp)
                    # get the documents from the file
                    new_docs = item.get("documents", [])
                    if "," in new_docs:
                        # if there are multiple, split into list
                        new_docs = new_docs.split(",")
                    else:
                        new_docs = [new_docs]
                    for doc in new_docs:
                        # get the @id of the document
                        doc_id = encodedcc.get_ENCODE(doc, connection)["@id"]
                        if doc_id not in documents:
                            # if not in list of documents from ENCODE, add it
                            documents.append(doc_id)

                    if any(item.get("notes", "")):
                        # get notes
                        notes = item["notes"]
        else:
            print("antibody", antibody.get("uuid"), "is not a primary characterization")
'''
if __name__ == '__main__':
        main()
