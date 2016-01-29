#!/usr/bin/env python
# -*- coding: latin-1 -*-
import argparse
import os
import datetime
import encodedcc
import logging
import sys

EPILOG = '''takes a file with a list of experiment accessions, a query,
or a single accession and checks all associated objects
and sets them to their released status'''
logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser(epilog=EPILOG)
    parser.add_argument('--infile',
                        help="File containing accessions or single accession")
    parser.add_argument('--outfile',
                        help="Output file name", default='Release_report.txt')
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('~/keypairs.json'))
    parser.add_argument('--update',
                        help="Run script and update the objects. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--query',
                        help="Custom query for accessions")
    parser.add_argument('--force',
                        help="Forces release of experiments that did not pass audit. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--debug',
                        help="Run script in debug mode.  Default is off",
                        action='store_true', default=False)
    parser.add_argument('--logall',
                        help="Adds status of 'released' objects to output file. Default is off",
                        action='store_true', default=False)
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.DEBUG)
    else:  # use the default logging level
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(message)s',
                            level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return args


class Data_Release():
    def __init__(self, args, connection):
        self.infile = args.infile
        self.outfile = args.outfile
        self.QUERY = args.query
        self.LOGALL = args.logall
        self.FORCE = args.force
        self.UPDATE = args.update
        self.keysLink = []
        self.PROFILES = {}
        self.ACCESSIONS = []
        self.statusDict = {}
        self.connection = connection
        temp = encodedcc.get_ENCODE("/profiles/", self.connection)
        ignore = ["AnalysisStepRun", "AnalysisStepVersion", "AnalysisStep",
                  "Lab", "Document", "Award", "AntibodyCharacterization",
                  "Publication", "Organism", "Reference", "AccessKey", "User"]
        self.profilesJSON = list(set(temp.keys()) - set(ignore))
        self.profiles_ref = []
        for profile in self.profilesJSON:
            if profile.endswith("y"):
                # this is a hack to find words like "Library"
                profile = profile.rstrip("y") + "ies"
                self.profiles_ref.append(profile.lower())
            elif profile.endswith("s"):
                # check for things with "Series" type endings
                self.profiles_ref.append(profile.lower())
            else:
                self.profiles_ref.append(profile.lower() + "s")

        for item in self.profilesJSON:
            profile = temp[item]
            self.keysLink = []  # if a key is in this list, it points to a link and will be embedded in the final product
            self.make_profile(profile)
            self.PROFILES[item] = self.keysLink

        self.current = []
        self.finished = []
        for item in temp.keys():
            status = temp[item]["properties"]["status"]["enum"]
            if "current" in status:
                self.current.append(item)
            if "finished" in status:
                self.finished.append(item)

    def make_profile(self, dictionary):
        '''builds the PROFILES reference dictionary
        keysLink is the list of keys that point to links, used in the PROFILES'''
        d = dictionary["properties"]
        for prop in d.keys():
            if d[prop].get("linkTo") or d[prop].get("linkFrom"):
                self.keysLink.append(prop)
            else:
                if d[prop].get("items"):
                    i = d[prop].get("items")
                    if i.get("linkTo") or i.get("linkFrom"):
                        self.keysLink.append(prop)

    def get_status(self, obj):
        '''take object get status, @type, @id, uuid
        {@id : [@type, status]}'''
        name = obj["@type"][0]
        self.searched.append(obj["@id"])
        if self.PROFILES.get(name):
            self.statusDict[obj["@id"]] = [name, obj["status"]]
            for key in obj.keys():
                # loop through object properties
                if key in self.PROFILES[name]:
                    # if the key is in profiles it's a link
                    if type(obj[key]) is list:
                        for link in obj[key]:
                            item = link.split("/")[1].replace("-", "")
                            #print(item)
                            if item in self.profiles_ref and link not in self.searched:
                                # expand subobject
                                subobj = encodedcc.get_ENCODE(link, self.connection)
                                self.get_status(subobj)
                    else:
                        item = obj[key].split("/")[1].replace("-", "")
                        #print(item)
                        if item in self.profiles_ref and obj[key] not in self.searched:
                            # expand subobject
                            subobj = encodedcc.get_ENCODE(obj[key], self.connection)
                            self.get_status(subobj)

    def releasinator(self, name, identifier, status, audit):
        '''releases objects into their equivalent released states'''
        patch_dict = {}
        if name in self.current:
            logger.info('%s' % "UPDATING: {} {} with status {} is now current".format(name, identifier, status))
            patch_dict = {"status": "current"}
        elif name in self.finished:
            logger.info('%s' % "UPDATING: {} {} with status {} is now finished".format(name, identifier, status))
            patch_dict = {"status": "finished"}
        else:
            log = "UPDATING: {} {} with status {} is now released".format(name, identifier, status)
            patch_dict = {"status": "released"}
            if audit:
                now = datetime.datetime.now().date()
                patch_dict = {"date_released": str(now), "status": "released"}
                log = "UPDATING: {} {} with status {} is now released with date {}".format(name, identifier, status, now)
            logger.info('%s' % log)
        encodedcc.patch_ENCODE(identifier, self.connection, patch_dict)

    def run_script(self):
        if self.UPDATE:
            print("WARNING: This run is an UPDATE run objects will be released.")
        else:
            print("Object status will be checked but not changed")
        if self.FORCE:
            print("WARNING: Objects that do not pass audit will be FORCE-released")
        if self.LOGALL:
            print("Logging all statuses")
        if self.infile:
            if os.path.isfile(self.infile):
                self.ACCESSIONS = [line.rstrip('\n') for line in open(self.infile)]
            else:
                self.ACCESSIONS = [self.infile]
        elif self.QUERY:
            if "search" in self.QUERY:
                temp = encodedcc.get_ENCODE(self.QUERY, self.connection).get("@graph", [])
            else:
                temp = [encodedcc.get_ENCODE(self.QUERY, self.connection)]
            if any(temp):
                for obj in temp:
                    if obj.get("accession"):
                        self.ACCESSIONS.append(obj["accession"])
                    elif obj.get("uuid"):
                        self.ACCESSIONS.append(obj["uuid"])
                    elif obj.get("@id"):
                        self.ACCESSIONS.append(obj["@id"])
                    elif obj.get("aliases"):
                        self.ACCESSIONS.append(obj["aliases"][0])
        if len(self.ACCESSIONS) == 0:
            print("ERROR: object has no identifier", file=sys.stderr)
            sys.exit(1)

        good = ["released", "current", "disabled", "published", "finished", "virtual"]
        bad = ["replaced", "revoked", "deleted", "upload failed",
               "format check failed", "uploading", "error"]
        ignore = ["User", "AntibodyCharacterization", "Publication"]
        for accession in self.ACCESSIONS:
            self.searched = []
            expandedDict = encodedcc.get_ENCODE(accession, self.connection)
            objectStatus = expandedDict.get("status")
            obj = expandedDict["@type"][0]

            audit = encodedcc.get_ENCODE(accession, self.connection, "page").get("audit", {})
            passAudit = True
            logger.info('%s' % "{}: {} Status: {}".format(obj, accession, objectStatus))
            if audit.get("ERROR", ""):
                logger.warning('%s' % "WARNING: Audit status: ERROR")
                passAudit = False
            if audit.get("NOT_COMPLIANT", ""):
                logger.warning('%s' % "WARNING: Audit status: NOT COMPLIANT")
                passAudit = False
            self.statusDict = {}
            self.get_status(expandedDict)
            if self.FORCE:
                passAudit = True

            named = []
            for key in sorted(self.statusDict.keys()):
                name = self.statusDict[key][0]
                status = self.statusDict[key][1]
                if name not in ignore:
                    if name not in named:
                        logger.info('%s' % name.upper())
                    if status in good:
                        if self.LOGALL:
                            logger.info('%s' % "{} has status {}".format(key, status))
                    elif status in bad:
                        logger.warning('%s' % "WARNING: {} has status {}".format(key, status))
                    else:
                        logger.info('%s' % "{} has status {}".format(key, status))
                        if self.UPDATE:
                            if passAudit:
                                self.releasinator(name, key, status, passAudit)
                    named.append(name)
        print("Data written to file", self.outfile)


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on", key.server)
    # build the PROFILES reference dictionary
    release = Data_Release(args, connection)
    release.run_script()

if __name__ == "__main__":
    main()
