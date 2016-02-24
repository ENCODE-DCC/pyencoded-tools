#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os.path
import csv
import encodedcc
from urllib.parse import quote

EPILOG = '''
Given a TSV file this script will attempt to add in the information
to the antibodies, the file is provided by the user

Example TSV file:
@id    lanes    lane_status         notes    documents
someID  2,3     compliant           get it?  important_document.pdf
someID  1,4     not compliant       got it   important_document.pdf
someID  5       pending dcc review  good     important_document.pdf


Useage:

    %(prog)s --infile MyFile.txt --user 4eg4-some-uuid-ks87
    %(prog)s --infile MyFile.txt --user /users/some-user

    Either a uuid or an @id can be used for user identification

This is a dryrun default script, run with '--update' to make changes

For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--user',
                        help="User uuid or @id for updating.")
    parser.add_argument('--infile',
                        help="TSV with headers of @id, lanes, lane_status, notes, documents\
                        this is created and filled out by the wrangler")
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
    print("Running on", connection.server)
    if args.update:
        assert args.user, "A user must be provided to run this script!"
        user = encodedcc.get_ENCODE(args.user, connection).get("@id")
        assert user, "{} was not found in the ENCODE database as a registered user. Please try again".format(args.user)

    data = []
    idList = []
    with open(args.infile, "r") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            data.append(row)
    for item in data:
        lanes = item.get("lanes", "")
        lanes = list(set(lanes.split(",")))
        item["lanes"] = lanes
        if not any(item["notes"]):
            item.pop("notes")
        if item.get("@id") not in idList:
            idList.append(item["@id"])
    objDict = {key: [] for key in idList}
    for item in data:
        objDict.get(item.get("@id", ""), "").append(item)

    for idNum in objDict.keys():
        antibody = encodedcc.get_ENCODE(idNum, connection, frame="edit")
        new_antibody = {}
        if antibody.get("primary_characterization_method"):
            reviews = antibody.get("characterization_reviews", [])
            enc_docs = antibody.get("documents", [])
            file_docs = []
            for obj in objDict[idNum]:
                if obj.get("documents"):
                    for doc in obj["documents"].split(","):
                        file_docs.append(doc)
                if obj.get("notes"):
                    new_antibody["notes"] = obj["notes"]
            for doc in file_docs:
                if ":" in doc:
                    doc = quote(doc)
                link = encodedcc.get_ENCODE(doc, connection).get("@id")
                if link:
                    if link not in enc_docs:
                        enc_docs.append(link)

            #######################
            # begin lanes checking
            #######################
            enc_lanes_check = []
            file_lanes_check = []
            flag = False
            for r in reviews:
                enc_lanes_check.append(r["lane"])
            for item in objDict[idNum]:
                for l in item["lanes"]:
                    file_lanes_check.append(int(l))
            if len(set(enc_lanes_check)) < len(enc_lanes_check):
                # duplicate lanes in ENCODE
                print("Possible duplicate lanes in ENCODE")
                flag = True
            if len(set(file_lanes_check)) < len(file_lanes_check):
                # duplicate lanes in file
                print("Possible duplicate lanes in file")
                flag = True
            if len(set(enc_lanes_check) - set(file_lanes_check)) > 0:
                # more lanes in ENCODE than in file
                print("Found lanes in ENCODE not in the file")
                flag = True
            if len(set(file_lanes_check) - set(enc_lanes_check)) > 0:
                # more lanes in file than in ENCODE
                print("Found lanes in the file not in ENCODE")
                flag = True
            if flag:
                print("Some problem was found with the number of lanes in the file as compared to ENCODE")
                print("Do you want to continue running the program or exit and check the data?")
                i = input("Continue? y/n ")
                assert i.upper() == "Y"
                # exit the script
            for r in reviews:
                for line in objDict[idNum]:
                    for lane in line["lanes"]:
                        if int(lane) == r["lane"]:
                            if line["lane_status"].lower() == "pending dcc review":
                                print("can't set to pending review, need manual override")
                                fin = input("Change the status to 'pending dcc review'? y/n ")
                                if fin.upper() == "Y":
                                    r["lane_status"] = line["lane_status"].lower()
                                    for link in enc_docs:
                                        if encodedcc.get_ENCODE(link, connection).get("document_type", "") == "standards document":
                                            enc_docs.pop(link)
                                else:
                                    pass
                            else:
                                r["lane_status"] = line["lane_status"].lower()
            # now all lanes in reviews should be updated to document
            enc_comp = 0
            enc_ncomp = 0
            other = 0

            for r in reviews:
                if r.get("lane_status", "") == "compliant":
                    enc_comp = enc_comp + 1
                elif r.get("lane_status", "") == "not compliant":
                    enc_ncomp = enc_ncomp + 1
                else:
                    other = other + 1
            if other > 0:
                print("not all lanes have allowed status, antibody characterization status set to not compliant")
                new_antibody["status"] = "not compliant"
            elif enc_comp > 0:
                new_antibody["status"] = "compliant"
            elif other == 0 and enc_comp == 0 and enc_ncomp > 0:
                new_antibody["status"] = "not compliant"
            ######################
            # end lanes checking
            ######################

            if antibody.get("lab", "") == "/labs/michael-snyder/":
                # make sure special document is added if not in the file
                if "michael-snyder:biorad_protein_standard" not in file_docs:
                    file_docs.append("michael-snyder:biorad_protein_standard")
                if antibody["primary_characterization_method"] == "immunoprecipitation":
                    if len(reviews) == 1:
                        # fix lane number
                        reviews[0]["lane"] = 3

            new_antibody["characterization_reviews"] = reviews
            new_antibody["documents"] = enc_docs
            if args.update:
                new_antibody["reviewed_by"] = user

        if args.update:
            print("PATCHing antibody characterization", idNum)
            encodedcc.patch_ENCODE(idNum, connection, new_antibody)
        else:
            print("PATCH data:", new_antibody)

if __name__ == '__main__':
        main()
