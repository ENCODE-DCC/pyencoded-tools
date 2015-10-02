from Bio import Entrez
from Bio import Medline
import argparse
import os
import csv
import logging
import encodedcc

logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--consortium',
                        help="File with consortium publication information")
    parser.add_argument('--community',
                        help="File with community publication information")
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
    parser.add_argument('email',
                        help="Email needed to make queries to Entrez process")
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.DEBUG)
    else:  # use the defaulf logging level
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(levelname)s:%(message)s')

    return args


class Publication_Update:
    def __init__(self, arguments):
        self.MAPPING = {"abstract": "AB", "authors": "AU", "title": "TI",
                        "volume": "VI", "journal": "JT", "date_published": "DP",
                        "page": "PG", "issue": "IP"}
        # These values will be filled when the code runs #
        self.entrezDict = {}
        self.PATCH_COUNT = 0
        self.POST_COUNT = 0
        #############################
        args = arguments
        self.UPDATE = args.update
        self.CREATE = args.create or args.createonly
        self.CREATE_ONLY = args.createonly
        self.UPDATE_ONLY = args.updateonly

    def getEntrez(self, idList):
        '''gets the values from Entrez
        '''
        handle = Entrez.efetch(db="pubmed", id=idList,
                               rettype="medline", retmode="text")
        # records is an iterator, so you can iterate through the records only once
        records = Medline.parse(handle)
        # save the records, you can convert them to a list
        records = list(records)
        for record in records:
            tempDict = {}
            for key in self.MAPPING.keys():
                if key == "authors":
                    auth = ", ".join(str(x) for x in record.get("AU", []))
                    tempDict["authors"] = auth
                else:
                    tempDict[key] = record.get(self.MAPPING.get(key), "")
            self.entrezDict[record.get("PMID")] = tempDict

    def checkENCODE(self, idList, connection, otherIdList=[], bothDicts={}):
        for pmid in idList:
            extraData = bothDicts.get(pmid)
            ENCODEvalue = encodedcc.get_ENCODE("/search/?type=publication&searchTerm=PMID:" + pmid, connection)
            if ENCODEvalue.get("@graph"):
                log = "PMID " + pmid + " is listed in ENCODE"
                logger.info('%s' % log)
                uuid = ENCODEvalue.get("@graph")[0].get("uuid")
                if not self.CREATE_ONLY:
                    self.compareEntrezENCODE(uuid, pmid, connection, extraData)
            else:
                if self.CREATE_ONLY:
                    self.getEntrez([pmid])
                titleEntrez = self.entrezDict[pmid].get("title")
                found = False
                for otherID in otherIdList:
                    titleENCODE = encodedcc.get_ENCODE(otherID, connection)
                    if titleENCODE.get("title") == titleEntrez:
                        log = pmid + " is in ENCODE by a different name " + titleENCODE.get("uuid")
                        logger.info('%s' % log)
                        self.compareEntrezENCODE(titleENCODE.get("uuid"), pmid, connection, extraData)
                        if self.UPDATE:
                            newIdent = titleENCODE.get("identifiers")
                            newIdent.append("PMID:" + pmid)
                            patch_dict = {"identifiers": newIdent}
                            encodedcc.patch_ENCODE(titleENCODE.get("uuid"), connection, patch_dict)
                        found = True
                if found is False:
                    log = "This publication is not listed in ENCODE " + pmid
                    logger.info('%s' % log)
                    if self.UPDATE and self.CREATE:
                        self.POST_COUNT += 1
                        pmidData = self.entrezDict[pmid]
                        log = "POSTing the new object: " + pmid
                        logger.info('%s' % log)
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
                        encodedcc.new_ENCODE(connection, "publications", post_dict)

    def compareEntrezENCODE(self, uuid, pmid, connection, extraData={}):
        '''compares value in ENCODE database to results from Entrez
        '''
        encode = encodedcc.get_ENCODE(uuid, connection)
        entrez = self.entrezDict.get(pmid)
        patch = False
        if not entrez:
            log = "WARNING!!: PMID " + pmid + " was not found in Entrez database!!"
            logger.warning('%s' % log)
        else:
            for key in entrez.keys():
                if key in encode.keys():
                    if entrez[key] == encode[key]:
                        log = "entrez key \"" + key + "\" matches encode key"
                        logger.info('%s' % log)
                    else:
                        log = "\"" + key + "\" value in encode database does not match value in entrez database"
                        logger.info('%s' % log)
                        log = "\tENTREZ: " + entrez[key] + "\n\tENCODE: " + encode[key]
                        logger.info('%s' % log)
                        if self.UPDATE or self.UPDATE_ONLY:
                            log = "PATCH in the new value for \"" + key + "\""
                            logger.info('%s' % log)
                            patch_dict = {key: entrez[key]}
                            encodedcc.patch_ENCODE(uuid, connection, patch_dict)
                            patch = True
                else:
                    log = "ENCODE missing \"" + key + "\" from Entrez.  New key and value must be added"
                    logger.warning('%s' % log)
                    if self.UPDATE or self.UPDATE_ONLY:
                        log = "PATCHing in new key \"" + key + "\""
                        logger.info('%s' % log)
                        patch_dict = {key: entrez[key]}
                        encodedcc.patch_ENCODE(uuid, connection, patch_dict)
                        patch = True
            if not self.UPDATE_ONLY:
                if extraData.get("categories"):
                    if set(encode.get("categories", [])) == set(extraData["categories"]):
                        log = "encode \"categories\" matches data in file"
                        logger.info('%s' % log)
                    else:
                        log = "encode \"categories\" \"" + ",".join(encode.get("categories", [])) + \
                            "\" does not match file \"" + \
                            ",".join(extraData["categories"]) + "\""
                        logger.warning('%s' % log)
                        if self.UPDATE:
                            if any(extraData["categories"]):
                                patch_dict = {"categories": extraData["categories"]}
                                encodedcc.patch_ENCODE(uuid, connection, patch_dict)
                                patch = True
                            else:
                                log = "No value in file to input for categories"
                                logger.warning('%s' % log)
                else:
                    log = "No value in file for categories, or value is not allowed"
                    logger.warning('%s' % log)
                if extraData.get("published_by"):  # this is to check that there is a value here, as there may not be values in the file
                    if set(encode.get("published_by", [])) == set(extraData["published_by"]):
                        log = "encode \"published_by\" matches data in file"
                        logger.info('%s' % log)
                    else:
                        log = "encode \"published_by\" \"" + ",".join(encode.get("published_by", [])) + \
                            "\" does not match data in file \"" + \
                            ",".join(extraData["published_by"]) + "\""
                        logger.warning('%s' % log)
                        if self.UPDATE:
                            if any(extraData["published_by"]):
                                patch_dict = {"published_by": extraData["published_by"]}
                                encodedcc.patch_ENCODE(uuid, connection, patch_dict)
                                patch = True
                            else:
                                log = "No value in file to input for published_by"
                                logger.warning('%s' % log)
                else:
                    log = "No value in file for published_by, or value is not allowed"
                    logger.warning('%s' % log)
                if extraData.get("data_used"):
                    if encode.get("data_used", "") == extraData["data_used"]:
                        log = "encode \"data_used\" matches data in file"
                        logger.info('%s' % log)
                    else:
                        log = "encode \"data_used\" \"" + encode.get("published_by", "") + \
                            "\" does not match data in file \"" + \
                            ",".join(extraData["data_used"]) + "\""
                        logger.info('%s' % log)
                        if self.UPDATE:
                            patch_dict = {"data_used": extraData["data_used"]}
                            encodedcc.patch_ENCODE(uuid, connection, patch_dict)
                            patch = True
            if encode.get("status", "") != "published" and (self.UPDATE or self.UPDATE_ONLY):
                log = "Setting status to published"
                logger.info('%s' % log)
                encodedcc.patch_ENCODE(uuid, connection, {"status": "published"})
                patch = True
            if patch is True:
                self.PATCH_COUNT += 1

    def findENCODEextras(self, communityList, consortiumList, connection):
        '''finds any publications in the ENCODE database
        that are not in the files provided
        '''
        community_url = "/search/?type=publication&status=published\
                        &published_by=community&field=identifiers&limit=all"
        consortium_url = "/search/?type=publication&status=published\
                        &published_by!=community&field=identifiers&limit=all"
        communityResult = encodedcc.get_ENCODE(community_url, connection).get("@graph")
        consortiumResult = encodedcc.get_ENCODE(consortium_url, connection).get("@graph")
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
    global MAPPING, UPDATE, CREATE, UPDATE_ONLY, CREATE_ONLY
    global entrezDict
    MAPPING = {"abstract": "AB", "authors": "AU", "title": "TI",
               "volume": "VI", "journal": "JT", "date_published": "DP",
               "page": "PG", "issue": "IP"}
    # These values will be filled when the code runs #
    entrezDict = {}
    PATCH_COUNT = 0
    POST_COUNT = 0
    #############################
    args = getArgs()
    community = args.community
    consortium = args.consortium
    outfile = args.outfile
    UPDATE = args.update
    CREATE = args.create or args.createonly
    CREATE_ONLY = args.createonly
    UPDATE_ONLY = args.updateonly
    Entrez.email = args.email
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    print("Running on ", connection.server)

    publication = Publication_Update(args)

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
            publication.getEntrez(pmidList)

        community_ENCODE_Only, communityOtherID, consortium_ENCODE_Only, consortiumOtherID = publication.findENCODEextras(community_ids, consortium_ids, connection)
        total_ENCODE_only = len(community_ENCODE_Only) + len(consortium_ENCODE_Only)
        allOtherIDs = communityOtherID + consortiumOtherID
        publication.checkENCODE(pmidList, connection, allOtherIDs, mergeDicts)
        log = str(total_ENCODE_only) + " items in ENCODE but not in files"
        logger.info('%s' % log)
        log = str(PATCH_COUNT) + " publication files PATCHed"
        logger.info('%s' % log)
        log = str(POST_COUNT) + " publication files POSTed"
        logger.info('%s' % log)
        print("Results printed to", outfile)
    else:
        infile = UPDATE_ONLY
        with open(infile, 'r') as readfile:
            pmidList = [x.rstrip('\n') for x in readfile]
        publication.getEntrez(pmidList)
        with open("ENCODE_update.txt", "w") as f:
            for pmid in pmidList:
                val = "/search/?searchTerm=PMID:" + pmid
                ENCODEvalue = encodedcc.get_ENCODE(val, connection)
                if ENCODEvalue.get("@graph"):
                    uuid = ENCODEvalue['@graph'][0].get("uuid")
                    publication.compareEntrezENCODE(uuid, pmid, connection)
            f.write(str(len(pmidList)) + " publications checked " + str(PATCH_COUNT) + " publications PATCHed")

if __name__ == '__main__':
    main()
