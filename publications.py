from Bio import Entrez
from Bio import Medline
from urllib.parse import urljoin
import requests
import argparse
import os
import json
import csv

global AUTHID, AUTHPW, SERVER, HEADERS, DEBUG, MAPPING, UPDATE, CREATE, UPDATE_ONLY
HEADERS = {'content-type': 'application/json', 'accept': 'application/json'}
MAPPING = {"abstract": "AB", "authors": "AU", "title": "TI", "volume": "VI",
           "journal": "JT", "date_published": "DP", "page": "PG", "issue": "IP"}
# These values will be filled when the code runs #
entrezDict = {}
PATCH_COUNT = 0
POST_COUNT = 0
#############################


def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--consortium',
                        help="File containing consortium publication information")
    parser.add_argument('--community',
                        help="File containing community publication information")
    parser.add_argument('--outfile',
                        help="Output file name", default='output.txt')
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('./keypairs.json'))
    parser.add_argument('--debug',
                        help="Debug prints out HTML requests and returned JSON \
                        objects. Default is off",
                        action='store_true',
                        default=False)
    parser.add_argument('--update',
                        help="Run script and PATCH objects as needed. \
                        Default is off",
                        action='store_true',
                        default=False)
    parser.add_argument('--create',
                        help="Run script and POST new objects as needed,\
                        must be run with --update.  Default is off",
                        action='store_true',
                        default=False)
    parser.add_argument('--createonly',
                        help="Run script and POST new objects as needed,\
                        only look up as needed.  Default is off",
                        action='store_true',
                        default=False)
    parser.add_argument('--updateonly',
                        help="File containing PMIDs from ENCODE database for\
                        updating. PMIDs ONLY!")
    parser.add_argument('--verbose',
                        help="Reports all results from run.\
                        Default only reports changes to be made.",
                        action='store_true',
                        default=False)
    parser.add_argument('email',
                        help="Email needed to make queries to Entrez process")
    args = parser.parse_args()
    return args


def processkey(key, keyfile=None):
    if not keyfile:
        try:
            keyfile = KEYFILE
        except:
            print("No keyfile was specified.")
            raise
    if key:
        keysf = open(keyfile, 'r')
        keys_json_string = keysf.read()
        keysf.close()
        keys = json.loads(keys_json_string)
        key_dict = keys[key]
    else:
        key_dict = {}
    AUTHID = key_dict.get('key')
    AUTHPW = key_dict.get('secret')
    if key:
        SERVER = key_dict.get('server')
    else:
        SERVER = DEFAULT_SERVER
    if not SERVER.endswith("/"):
        SERVER += "/"
    return (AUTHID, AUTHPW, SERVER)


def get_ENCODE(uri):
    '''GET an ENCODE object as JSON and return as dict'''
    if '?' in uri:  # have to do this because it might be the first directive in the URL
        uri += '&frame=object'
    else:
        uri += '?frame=object'
    url = urljoin(SERVER, uri)
    if DEBUG:
        print("DEBUG: GET %s" % (url))
    response = requests.get(url, auth=(AUTHID, AUTHPW), headers=HEADERS)
    if DEBUG:
        print("DEBUG: GET RESPONSE code %s" % (response.status_code))
        try:
            if response.json():
                print("DEBUG: GET RESPONSE JSON")
                print(json.dumps(response.json(), indent=4, separators=(',', ': ')))
        except:
            print("DEBUG: GET RESPONSE text %s" % (response.text))
    if not response.status_code == 200:
        print(response.text)
    return response.json()


def patch_ENCODE(obj_id, patch_input):
    '''PATCH an existing ENCODE object and return the response JSON
    '''
    if isinstance(patch_input, dict):
        json_payload = json.dumps(patch_input)
    elif isinstance(patch_input, str):
        json_payload = patch_input
    else:
        print('Datatype to patch is not string or dict.')
    url = urljoin(SERVER, obj_id)
    if DEBUG:
        print("DEBUG: PATCH URL: %s" % (url))
        print("DEBUG: PATCH data: %s" % (json_payload))
    response = requests.patch(url, auth=(AUTHID, AUTHPW), data=json_payload, headers=HEADERS)
    if DEBUG:
        print("DEBUG: PATCH RESPONSE %s" % (response.status_code))
        try:
            if response.json():
                print("DEBUG: GET RESPONSE JSON")
                print(json.dumps(response.json(), indent=4, separators=(',', ': ')))
        except:
            print("DEBUG: GET RESPONSE text %s" % (response.text))
    if not response.status_code == 200:
        print(response.text)
    return response.json()


def post_ENCODE(collection_id, post_input):
    '''POST an ENCODE object as JSON and return the response JSON
    '''
    if isinstance(post_input, dict):
        json_payload = json.dumps(post_input)
    elif isinstance(post_input, str):
        json_payload = post_input
    else:
        print('Datatype to post is not string or dict.')
    url = urljoin(SERVER, collection_id)
    if DEBUG:
        print("DEBUG: POST URL : %s" % (url))
        print("DEBUG: POST data: %s" % (json_payload))
        print(json.dumps(post_input, sort_keys=True, indent=4, separators=(',', ': ')))
    response = requests.post(url, auth=(AUTHID, AUTHPW), headers=HEADERS, data=json_payload)
    if DEBUG:
        print("DEBUG: POST RESPONSE %s" % (response.status_code))
        try:
            if response.json():
                print("DEBUG: GET RESPONSE JSON")
                print(json.dumps(response.json(), indent=4, separators=(',', ': ')))
        except:
            print("DEBUG: GET RESPONSE test %s" % (response.text))
    if not response.status_code == 201:
        print(response.text)
    return response.json()


def getEntrez(idList):
    '''gets the values from Entrez
    '''
    handle = Entrez.efetch(db="pubmed", id=idList, rettype="medline", retmode="text")
    records = Medline.parse(handle)  # records is an iterator, so you can iterate through the records only once
    records = list(records)  # save the records, you can convert them to a list
    for record in records:
        tempDict = {}
        for key in MAPPING.keys():
            if key == "authors":
                auth = ", ".join(str(x) for x in record.get("AU", []))
                tempDict["authors"] = auth
            else:
                tempDict[key] = record.get(MAPPING.get(key), "")
        entrezDict[record.get("PMID")] = tempDict


def checkENCODE(idList, otherIdList=[], bothDicts={}):
    for pmid in idList:
        extraData = bothDicts.get(pmid)
        ENCODEvalue = get_ENCODE("/search/?type=publication&searchTerm=PMID:" + pmid)
        if ENCODEvalue.get("@graph"):
            if VERBOSE:
                f.write("PMID " + pmid + " is listed in ENCODE\n")
            uuid = ENCODEvalue.get("@graph")[0].get("uuid")
            if not CREATE_ONLY:
                compareEntrezENCODE(uuid, pmid, extraData)
        else:
            if CREATE_ONLY:
                getEntrez([pmid])
            titleEntrez = entrezDict[pmid].get("title")
            found = False
            for otherID in otherIdList:
                titleENCODE = get_ENCODE(otherID)
                if titleENCODE.get("title") == titleEntrez:
                    f.write(pmid + " is in ENCODE by a different name " + titleENCODE.get("uuid") + "\n")
                    compareEntrezENCODE(titleENCODE.get("uuid"), pmid, extraData)
                    if UPDATE:
                        newIdent = titleENCODE.get("identifiers")
                        newIdent.append("PMID:" + pmid)
                        patch_dict = {"identifiers": newIdent}
                        patch_ENCODE(titleENCODE.get("uuid"), patch_dict)
                    found = True
            if found is False:
                f.write("This publication is not listed in ENCODE " + pmid + "\n")
                if UPDATE and CREATE:
                    POST_COUNT += 1
                    pmidData = entrezDict[pmid]
                    f.write("POSTing the new object: " + pmid + "\n")
                    post_dict = {
                        "title": pmidData.get("title"),
                        "abstract": pmidData.get("abstract"),
                        "submitted_by": "/users/8b1f8780-b5d6-4fb7-a5a2-ddcec9054288/",
                        "lab": "/labs/encode-consortium/",
                        "award": "/awards/ENCODE/",
                        "categories": extraData.get("categories"),
                        "published_by": extraData.get("published_by"),
                        "date_published": pmidData.get("date_published"),
                        "authors": pmidData.get("authors"),
                        "identifiers": ["PMID:" + pmid],
                        "journal": pmidData.get("journal"),
                        "volume": pmidData.get("volume"),
                        "issue": pmidData.get("issue"),
                        "page": pmidData.get("page"),
                        "status": "published"
                    }
                    if extraData.get("data_used"):
                        post_dict["data_used"] = extraData.get("data_used")
                    post_ENCODE("publications", post_dict)


def compareEntrezENCODE(uuid, pmid, extraData={}):
    '''compares value in ENCODE database to results from Entrez
    '''
    encode = get_ENCODE(uuid)
    entrez = entrezDict.get(pmid)
    patch = False
    if not entrez:
        f.write("WARNING!!: PMID " + pmid + " was not found in Entrez database!!\n")
    else:
        for key in entrez.keys():
            if key in encode.keys():
                if entrez[key] == encode[key]:
                    if VERBOSE:
                        f.write("entrez key \"" + key + "\" matches encode key\n")
                else:
                    f.write("\"" + key + "\" value in encode database does not match value in entrez database\n")
                    f.write("\tENTREZ: " + entrez[key] + "\n\tENCODE: " + encode[key] + "\n")
                    if UPDATE or UPDATE_ONLY:
                        f.write("PATCH in the new value for \"" + key + "\"\n")
                        patch_dict = {key: entrez[key]}
                        patch_ENCODE(uuid, patch_dict)
                        patch = True
            else:
                f.write("ENCODE missing \"" + key + "\" from Entrez.  New key and value must be added\n")
                if UPDATE or UPDATE_ONLY:
                    f.write("PATCHing in new key \"" + key + "\"\n")
                    patch_dict = {key: entrez[key]}
                    patch_ENCODE(uuid, patch_dict)
                    patch = True
        if not UPDATE_ONLY:
            if extraData.get("categories"):
                if set(encode.get("categories", [])) == set(extraData.get("categories")):
                    if VERBOSE:
                        f.write("encode \"categories\" matches data in file\n")
                else:
                    f.write("encode \"categories\" \"" + ",".join(encode.get("categories", [])) + "\" does not match file \"" + ",".join(extraData.get("categories")) + "\"\n")
                    if UPDATE:
                        if any(extraData.get("categories", [])):
                            patch_dict = {"categories": extraData.get("categories")}
                            patch_ENCODE(uuid, patch_dict)
                            patch = True
                        else:
                            f.write("No value in file to input for categories\n")
            else:
                f.write("No value in file for categories, or value is not allowed\n")
            if extraData.get("published_by"):  # this is to check that there is a value here, as there may not be values in the file
                if set(encode.get("published_by", [])) == set(extraData.get("published_by")):
                    if VERBOSE:
                        f.write("encode \"published_by\" matches data in file\n")
                else:
                    f.write("encode \"published_by\" \"" + ",".join(encode.get("published_by", [])) + "\" does not match data in file \"" + ",".join(extraData.get("published_by")) + "\"\n")
                    if UPDATE:
                        if any(extraData.get("published_by", [])):
                            patch_dict = {"published_by": extraData.get("published_by")}
                            patch_ENCODE(uuid, patch_dict)
                            patch = True
                        else:
                            f.write("No value in file to input for published_by\n")
            else:
                f.write("No value in file for published_by, or value is not allowed\n")
            if extraData.get("data_used"):
                if encode.get("data_used", "") == extraData.get("data_used", ""):
                    if VERBOSE:
                        f.write("encode \"data_used\" matches data in file")
                else:
                    f.write("encode \"data_used\" \"" + encode.get("published_by", "") + "\" does not match data in file \"" + extraData.get("data_used") + "\"\n")
                    if UPDATE:
                        patch_dict = {"data_used": extraData.get("data_used")}
                        patch_ENCODE(uuid, patch_dict)
                        patch = True
        if encode.get("status", "") != "published" and (UPDATE or UPDATE_ONLY):
            f.write("Setting status to published")
            patch_ENCODE(uuid, {"status": "published"})
            patch = True
        global PATCH_COUNT
        if patch is True:
            PATCH_COUNT += 1


def findENCODEextras(communityList, consortiumList):
    '''finds any publications in the ENCODE database that are not in the files provided
    '''
    community_url = "/search/?type=publication&status=published&published_by=community&field=identifiers&limit=all"
    consortium_url = "/search/?type=publication&status=published&published_by!=community&field=identifiers&limit=all"
    communityResult = get_ENCODE(community_url).get("@graph")
    consortiumResult = get_ENCODE(consortium_url).get("@graph")
    communityPMIDfromENCODE = []  # list of PMID from ENCODE site
    communityOtherID = []  # list of non-PMID ids from ENCODE site
    for pub in communityResult:
        temp = pub.get("identifiers", [])
        for idNum in temp:
            if "PMID:" in idNum:
                communityPMIDfromENCODE.append(idNum)
                # this is something that has a pubmed ID
            elif "PMCID:PMC" in idNum:
                pass
                # this is an alternate PMID
            else:
                uuid = pub.get("@id")
                communityOtherID.append(uuid)
                # this is something that does not have a PMID yet, find it and PATCH it in
    community_ENCODE_Only = list(set(communityPMIDfromENCODE) - set(communityList))
    consortiumPMIDfromENCODE = []  # list of PMID from ENCODE site
    consortiumOtherID = []  # list of non-PMID ids from ENCODE site
    for pub in consortiumResult:
        temp = pub.get("identifiers", [])
        for idNum in temp:
            if "PMID:" in idNum:
                consortiumPMIDfromENCODE.append(idNum)
                # this is something that has a pubmed ID
            elif "PMCID:PMC" in idNum:
                pass
                # this is an alternate PMID
            else:
                uuid = pub.get("@id")
                consortiumOtherID.append(uuid)
                # this is something that does not have a PMID yet, find it and PATCH it in
    consortium_ENCODE_Only = list(set(consortiumPMIDfromENCODE) - set(consortiumList))
    return community_ENCODE_Only, communityOtherID, consortium_ENCODE_Only, consortiumOtherID


def main():
    args = getArgs()
    AUTHID, AUTHPW, SERVER = processkey(args.key, args.keyfile)
    keypair = (AUTHID, AUTHPW)
    DEBUG = args.debug
    community = args.community
    consortium = args.consortium
    outfile = args.outfile
    UPDATE = args.update
    CREATE = args.create or args.createonly
    CREATE_ONLY = args.createonly
    UPDATE_ONLY = args.updateonly
    VERBOSE = args.verbose
    Entrez.email = args.email

    print("Running on ", SERVER)

    if not UPDATE_ONLY:
        if UPDATE:
            print("Will PATCH publication objects as needed")
        if CREATE:
            print("POST new pubmeds")

        '''consortium publications file'''
        consortium_dict = {}
        with open(consortium, 'r', encoding='ISO-8859-1') as f:
            reader = csv.reader(f, delimiter='\t')
            for PMID, published_by, categories, catch1, code, catch2, title in reader:
                categories = categories.replace(";", ",").rstrip(" ")
                published_by = published_by.replace(";", ",").rstrip(" ")
                cat = [x.strip(' ').lower() for x in categories.rstrip(',').split(",") if x != 'production']
                pub = [x.strip(' ') for x in published_by.rstrip(',').split(",")]
                temp = {"published_by": pub, "categories": cat, "title": title}
                consortium_dict[PMID] = temp
        consortium_ids = list(consortium_dict.keys())

        '''community publications file'''
        community_dict = {}
        with open(community, 'r', encoding='ISO-8859-1') as f:
            reader = csv.reader(f, delimiter='\t')
            for PMID, published_by, categories, data_used, catch1, catch2, title, catch3, catch4, catch5, catch6, catch7, catch8, catch9, catch10, catch11, catch12, catch13, catch14, catch15, catch16, catch17, catch18 in reader:
                categories = categories.replace(";", ",").rstrip(" ")
                published_by = published_by.replace(";", ",").rstrip(" ")
                cat = [x.strip(' ').lower() for x in categories.rstrip(',').split(",") if x != 'production']
                pub = [x.strip(' ') for x in published_by.rstrip(',').split(",")]
                temp = {"published_by": pub, "categories": cat, "title": title, "data_used": data_used}
                community_dict[PMID] = temp
        community_ids = list(community_dict.keys())

        pmidList = consortium_ids + community_ids
        mergeDicts = consortium_dict.copy()
        mergeDicts.update(community_dict)  # this dict now holds all the information regarding published_by and categories

        if not CREATE_ONLY:
            getEntrez(pmidList)

        community_ENCODE_Only, communityOtherID, consortium_ENCODE_Only, consortiumOtherID = findENCODEextras(community_ids, consortium_ids)
        total_ENCODE_only = len(community_ENCODE_Only) + len(consortium_ENCODE_Only)
        with open(outfile, "w") as f:
            checkENCODE(pmidList, communityOtherID + consortiumOtherID, mergeDicts)
            f.write(str(total_ENCODE_only) + " items in ENCODE but not in files\n")
            f.write(str(PATCH_COUNT) + " publication files PATCHed")
            f.write(str(POST_COUNT) + " publication files POSTed")
        print("Results printed to", outfile)
    else:
        with open(UPDATE_ONLY, 'r') as readfile:
            pmidList = [x.rstrip('\n') for x in readfile]
        getEntrez(pmidList)
        with open("ENCODE_update.txt", "w") as f:
            for pmid in pmidList:
                ENCODEvalue = get_ENCODE("/search/?type=publication&searchTerm=PMID:" + pmid)
                if ENCODEvalue.get("@graph"):
                    uuid = ENCODEvalue.get("@graph")[0].get("uuid")
                    if VERBOSE:
                        f.write("Checking PMID:" + pmid + "\n")
                    compareEntrezENCODE(uuid, pmid)
            f.write(str(len(pmidList)) + " publications checked " + str(PATCH_COUNT) + " publications PATCHed")

if __name__ == '__main__':
    main()
