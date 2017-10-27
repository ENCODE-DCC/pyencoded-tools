#!/usr/bin/env python3
# -*- coding: latin-1 -*-
from Bio import Entrez
from Bio import Medline
import argparse
import os
import csv
import logging
import encodedcc

EPILOG = '''
Takes in a VERY specific file format to use for updating the publications
Also can update the existing publications using the pubmed database

An EMAIL is required to run this script
This is for the Entrez database

This is a dryrun default script
This script requires the BioPython module

Options:

    %(prog)s --consortium Consortium_file.txt

This takes the consortium file

    %(prog)s --community Community_file.txt

This takes the community file

    %(prog)s --updateonly list.txt

Takes file with single column of publication UUIDs, checks against PubMed \
to ensure data is correct and will update if needed

'''

logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--consortium',
                        help="File with consortium publication information")
    parser.add_argument('--community',
                        help="File with community publication information")
    parser.add_argument('--outfile',
                        help="Output file name", default='publication_results.txt')
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('~/keypairs.json'))
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
                        help="Run script and POST new objects as needed.  \
                        Default is off",
                        action='store_true',
                        default=False)
    parser.add_argument('--createonly',
                        help="Run script and POST new objects as needed,\
                        only look up as needed.  Default is off",
                        action='store_true',
                        default=False)
    parser.add_argument('--updateonly',
                        help="File containing publication UUIDS from ENCODE database for\
                        updating.  If the publication does not have PMID the script will\
                        find it comparing based on title and assuming unique title")
    parser.add_argument('email',
                        help="Email needed to make queries to Entrez process")
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.DEBUG)
    else:  # use the default logging level
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return args


class PublicationUpdate:
    def __init__(self, arguments):
        self.MAPPING = {"abstract": "AB", "authors": "AU", "title": "TI",
                        "volume": "VI", "journal": "JT", "date_published": "DP",
                        "page": "PG", "issue": "IP"}
        self.entrezDict = {}
        self.PATCH_COUNT = 0
        self.POST_COUNT = 0
        args = arguments
        self.UPDATE = args.update
        self.CREATE = args.create or args.createonly
        self.CREATE_ONLY = args.createonly
        self.UPDATE_ONLY = args.updateonly
        self.community = args.community
        self.consortium = args.consortium
        if self.UPDATE:
            print("Will PATCH publication objects as needed")
        if self.CREATE:
            print("POST new pubmeds")

    def setup_publication(self):
        '''consortium publications file'''
        self.consortium_dict = {}
        with open(self.consortium, 'r', encoding='ISO-8859-1') as f:
            reader = csv.reader(f, delimiter='\t')
            for PMID, published_by, categories, catch1, code, catch2, title in reader:
                categories = categories.replace(";", ",").rstrip(" ")
                published_by = published_by.replace(";", ",").rstrip(" ")
                cat = [x.strip(' ').lower()
                       for x in categories.rstrip(',').split(",")]
                pub = [x.strip(' ')
                       for x in published_by.rstrip(',').split(",")]
                temp = {"published_by": pub, "categories": cat}
                self.consortium_dict[PMID] = temp
        self.consortium_ids = list(self.consortium_dict.keys())

        '''community publications file'''
        self.community_dict = {}
        with open(self.community, 'r', encoding='ISO-8859-1') as f:
            reader = csv.reader(f, delimiter='\t')
            for PMID, published_by, categories, data_used, catch1, catch2, title, catch3, catch4, catch5, catch6, catch7, catch8, catch9, catch10, catch11, catch12, catch13, catch14, catch15, catch16, catch17, catch18 in reader:
                categories = categories.replace(";", ",").rstrip(" ")
                published_by = published_by.replace(";", ",").rstrip(" ")
                cat = [x.strip(' ').lower()
                       for x in categories.rstrip(',').split(",")]
                pub = [x.strip(' ')
                       for x in published_by.rstrip(',').split(",")]
                temp = {"published_by": pub,
                        "categories": cat, "data_used": data_used}
                self.community_dict[PMID] = temp
        self.community_ids = list(self.community_dict.keys())

    def get_entrez(self, idList):
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

    def check_ENCODE(self, idList, connection, otherIdList=[], bothDicts={}):
        for pmid in idList:
            extraData = bothDicts.get(pmid)
            ENCODEvalue = encodedcc.get_ENCODE(
                "/search/?type=publication&searchTerm=PMID:" + pmid, connection)
            if ENCODEvalue.get("@graph"):
                log = "PMID " + pmid + " is listed in ENCODE"
                logger.info('%s' % log)
                uuid = ENCODEvalue.get("@graph")[0].get("uuid")
                if not self.CREATE_ONLY:
                    self.compare_entrez_ENCODE(
                        uuid, pmid, connection, extraData)
            else:
                if self.CREATE_ONLY:
                    self.get_entrez([pmid])
                titleEntrez = self.entrezDict[pmid].get("title")
                found = False
                for otherID in otherIdList:
                    titleENCODE = encodedcc.get_ENCODE(
                        "/search/?type=publication&searchTerm=" + otherID, connection)
                    if titleENCODE.get("title") == titleEntrez:
                        log = pmid + " is in ENCODE by a different name " + \
                            titleENCODE.get("uuid")
                        logger.warning('%s' % log)
                        self.compare_entrez_ENCODE(titleENCODE.get(
                            "uuid"), pmid, connection, extraData)
                        if self.UPDATE:
                            newIdent = titleENCODE.get("identifiers")
                            newIdent.append("PMID:" + pmid)
                            patch_dict = {"identifiers": newIdent}
                            encodedcc.patch_ENCODE(titleENCODE.get(
                                "uuid"), connection, patch_dict)
                        found = True
                if found is False:
                    log = "This publication is not listed in ENCODE " + pmid
                    logger.warning('%s' % log)
                    if self.CREATE:
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
                        encodedcc.new_ENCODE(
                            connection, "publications", post_dict)

    def compare_entrez_ENCODE(self, uuid, pmid, connection, extraData={}):
        '''compares value in ENCODE database to results from Entrez
        '''
        encode = encodedcc.get_ENCODE(uuid, connection)
        entrez = self.entrezDict.get(pmid)
        patch = False
        if not entrez:
            log = "PMID " + pmid + " was not found in Entrez database!!"
            logger.warning('%s' % log)
        else:
            log = "PMID " + pmid
            logger.info('%s' % log)
            for key in entrez.keys():
                if key in encode.keys():
                    if entrez[key] == encode[key]:
                        log = "entrez key \"" + key + "\" matches encode key"
                        logger.info('%s' % log)
                    else:
                        log = "\"" + key + "\" value in encode database does not match value in entrez database"
                        logger.warning('%s' % log)
                        log = "\tENTREZ: " + \
                            entrez[key] + "\n\tENCODE: " + encode[key]
                        logger.warning('%s' % log)
                        if self.UPDATE or self.UPDATE_ONLY:
                            log = "PATCH in the new value for \"" + key + "\""
                            logger.info('%s' % log)
                            patch_dict = {key: entrez[key]}
                            encodedcc.patch_ENCODE(
                                uuid, connection, patch_dict)
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
                for key in extraData.keys():
                    if type(extraData.get(key)) is list:
                        if set(encode.get(key, [])) == set(extraData.get(key, [])):
                            log = "encode \"" + key + "\" matches data in file"
                            logger.info('%s' % log)
                        else:
                            log = "encode \"" + key + "\" value" + \
                                str(encode.get(key, [])) + \
                                "does not match file"
                            logger.warning('%s' % log)
                            if self.UPDATE:
                                if any(extraData[key]):
                                    patch_dict = {key: extraData[key]}
                                    encodedcc.patch_ENCODE(
                                        uuid, connection, patch_dict)
                                    patch = True
                                else:
                                    log = "No value in file to input for \"" + key + "\""
                                    logger.warning('%s' % log)
                    if type(extraData.get(key)) is str:
                        if encode.get(key, "") == extraData.get(key, ""):
                            log = "encode \"" + key + "\" matches data in file"
                            logger.info('%s' % log)
                        else:
                            log = "encode \"" + key + "\" value" + \
                                str(encode.get(key, "")) + \
                                "does not match file"
                            logger.warning('%s' % log)
                            if self.UPDATE:
                                patch_dict = {key: extraData[key]}
                                encodedcc.patch_ENCODE(
                                    uuid, connection, patch_dict)
                                patch = True
            if encode.get("status", "") != "published" and (self.UPDATE or self.UPDATE_ONLY):
                log = "Setting status to published"
                logger.info('%s' % log)
                encodedcc.patch_ENCODE(
                    uuid, connection, {"status": "published"})
                patch = True
            if patch is True:
                self.PATCH_COUNT += 1

    def find_ENCODE_extras(self, communityList, consortiumList, connection):
        '''finds any publications in the ENCODE database
        that are not in the files provided
        '''
        community_url = "/search/?type=publication&status=published\
                        &published_by=community&field=identifiers&limit=all"
        consortium_url = "/search/?type=publication&status=published\
                        &published_by!=community&field=identifiers&limit=all"
        communityResult = encodedcc.get_ENCODE(
            community_url, connection).get("@graph")
        consortiumResult = encodedcc.get_ENCODE(
            consortium_url, connection).get("@graph")
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
        community_ENCODE_Only = list(
            set(communityPMIDfromENCODE) - set(communityList))
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
        consortium_ENCODE_Only = list(
            set(consortiumPMIDfromENCODE) - set(consortiumList))
        return community_ENCODE_Only, communityOtherID, consortium_ENCODE_Only, consortiumOtherID


def main():
    args = getArgs()
    outfile = args.outfile
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

    publication = PublicationUpdate(args)

    if not UPDATE_ONLY:
        publication.setup_publication()
        pmidList = publication.consortium_ids + publication.community_ids
        mergeDicts = publication.consortium_dict.copy()
        # holds published_by, categories, and data_used
        mergeDicts.update(publication.community_dict)

        if not CREATE_ONLY:
            publication.get_entrez(pmidList)

        community_ENCODE_Only, communityOtherID, consortium_ENCODE_Only, consortiumOtherID = publication.find_ENCODE_extras(
            publication.community_ids, publication.consortium_ids, connection)
        total_ENCODE_only = len(community_ENCODE_Only) + \
            len(consortium_ENCODE_Only)
        allOtherIDs = communityOtherID + consortiumOtherID
        publication.check_ENCODE(pmidList, connection, allOtherIDs, mergeDicts)
        log = str(total_ENCODE_only) + " items in ENCODE but not in files"
        logger.info('%s' % log)
        log = str(publication.PATCH_COUNT) + " publication files PATCHed"
        logger.info('%s' % log)
        log = str(publication.POST_COUNT) + " publication files POSTed"
        logger.info('%s' % log)
        print("Results printed to", outfile)
    else:
        infile = UPDATE_ONLY
        with open(infile, 'r') as readfile:
            uuidList = [x.rstrip('\n') for x in readfile]
        # check each publication to see if it has a PMID, if it does add it to the PMIDlist
        # if it does not have one look it up on Entrez
        pmid_uuid_dict = {}
        for uuid in uuidList:
            pub = encodedcc.get_ENCODE(uuid, connection)
            title = pub.get("title", "")
            identifiers = pub.get("identifiers", [])
            found = False
            for i in identifiers:
                if "PMID:" in i:
                    p = i.split(":")[1]
                    found = True
            if found:
                pmid_uuid_dict[p] = uuid
            else:
                # search Entrez for publication by title
                handle = Entrez.esearch(db="pubmed", term=title)
                record = Entrez.read(handle)
                idlist = record["IdList"]
                if len(idlist) > 1:
                    log = "More than one possible PMID found for " + uuid
                    logger.error('%s' % log)
                    log = str(idlist) + " are possible PMIDs"
                    logger.error('%s' % log)
                elif len(idlist) == 0:
                    log = "No possible PMID found for " + uuid
                    logger.error('%s' % log)
                else:
                    handle = Entrez.efetch(
                        db="pubmed", id=idlist, rettype="medline", retmode="text")
                    records = Medline.parse(handle)
                    # save the records, you can convert them to a list
                    records = list(records)
                    for record in records:
                        pm = record.get("PMID")
                        ti = record.get("TI")
                        log = "Publication " + uuid + " with title \"" + title + \
                            "\" matches PMID:" + pm + " with title \"" + ti + "\""
                        logger.info('%s' % log)
                        identifiers.append("PMID:" + pm)
                        encodedcc.patch_ENCODE(
                            uuid, connection, {"identifiers": identifiers})
                        pmid_uuid_dict[pm] = uuid
        pmidList = list(pmid_uuid_dict.keys())
        publication.get_entrez(pmidList)
        with open("pub_update.txt", "w") as f:
            for pmid in pmid_uuid_dict.keys():
                publication.compare_entrez_ENCODE(
                    pmid_uuid_dict[pmid], pmid, connection)
            f.write(str(len(pmid_uuid_dict.keys())) + " publications checked " +
                    str(publication.PATCH_COUNT) + " publications PATCHed")


if __name__ == '__main__':
    main()
