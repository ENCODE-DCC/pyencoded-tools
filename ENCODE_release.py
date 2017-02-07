#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os
import datetime
import encodedcc
import logging
import sys
import time
import hierarchy as hi

EPILOG = '''
%(prog)s is a script that will release objects fed to it
Default settings only report the status of releaseable objects
and will NOT release unless instructed
In addition if an object fails to pass the Error or Not Compliant
audits it will not be released

Basic Useage:

    %(prog)s --infile file.txt
    %(prog)s --infile ENCSR000AAA
    %(prog)s --infile ENCSR000AAA,ENCSR000AAB,ENCSR000AAC

    A single column file listing the  identifiers of the objects desired
    A single identifier or comma separated list of identifiers is also useable


    %(prog)s --query "/search/?type=Experiment&status=release+ready"

    A query may be fed to the script to use for the object list


    %(prog)s --infile file.txt --update

    '--update' should be used whenever you want to PATCH the changes
    to the database, otherwise the script will stop before PATCHing


    %(prog)s --infile file.txt --force --update

    if an object does not pass the 'Error' or 'Not Compliant' audit
    it can still be released with the '--force' option
    MUST BE RUN WITH '--update' TO WORK


    %(prog)s --infile file.txt --logall

    Default script will not log status of 'released' objects,
    using '--logall' will make it include the statuses of released items
    in the report file


Misc. Useage:

    The output file default is 'Release_report.txt'
    This can be changed with '--output'

    Default keyfile location is '~/keyfile.json'
    Change with '--keyfile'

    Default key is 'default'
    Change with '--key'

    Default debug is off
    Change with '--debug'

'''
logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--infile',
                        help="File containing single column of accessions " +
                        "or a single accession or comma separated list " +
                        "of accessions")
    parser.add_argument('--query',
                        help="Custom query for accessions")
    parser.add_argument('--update',
                        help="Run script and update the objects. Default is " +
                        "off",
                        action='store_true', default=False)
    parser.add_argument('--printall',
                        help="Prints to stdout objects that are being " +
                        "released. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--force',
                        help="Forces release of experiments that did not " +
                        "pass audit. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--logall',
                        help="Adds status of 'released' objects to output " +
                        "file. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--outfile',
                        help="Output file name", default='Release_report.txt')
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('~/keypairs.json'))
    parser.add_argument('--timing',
                        help="Time the script execution.  Default is off",
                        action='store_true', default=False)
    parser.add_argument('--debug',
                        help="Run script in debug mode.  Default is off",
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
        # renaming some things so I can be lazy and not pass them around
        self.releasenator_version = 1.2
        self.infile = args.infile
        self.outfile = args.outfile
        self.QUERY = args.query
        self.LOGALL = args.logall
        self.FORCE = args.force
        self.PRINTALL = args.printall
        self.UPDATE = args.update
        self.TIMING = args.timing
        self.keysLink = []
        self.PROFILES = {}
        self.ACCESSIONS = []
        self.statusDict = {}
        self.connection = connection
        temp = encodedcc.get_ENCODE("/profiles/", self.connection)

        ignore = ["Lab", "Award", "Platform",
                  "Organism", "Reference", "AccessKey", "User", "AnalysisStep",
                  "AnalysisStepVersion", "AnalysisStepRun", "Pipeline",
                  "Antibody", "AntibodyCharacterization", "AntibodyApproval"]
        self.profilesJSON = []
        self.date_released = []
        for profile in temp.keys():
            # get the names of things we DON'T expand
            # these things usually link to other experiments/objects
            if profile in ignore:
                pass
            else:
                self.profilesJSON.append(profile)
        self.profiles_ref = []
        for profile in self.profilesJSON:
            self.profiles_ref.append(self.helper(profile))
        for item in self.profilesJSON:
            profile = temp[item]  # getting the whole schema profile
            self.keysLink = []  # if a key is in this list, it points to a
            # link and will be embedded in the final product
            object_type = item
            if item in ['PublicationData',
                        'Reference',
                        'Series',
                        'UcscBrowserComposite',
                        'ReferenceEpigenome',
                        'MatchedSet',
                        'TreatmentTimeSeries',
                        'ReplicationTimingSeries',
                        'OrganismDevelopmentSeries',
                        'TreatmentConcentrationSeries',
                        'Annotation',
                        'Project',
                        'Experiment',
                        'Dataset',
                        'FileSet']:
                object_type = 'Dataset'
            self.make_profile(profile, object_type)
            self.PROFILES[item] = self.keysLink
            # lets get the list of things that actually get a date released
            for value in profile["properties"].keys():
                if value == "date_released":
                    self.date_released.append(item)

        self.current = []
        self.finished = []
        for item in temp.keys():
            status = temp[item]["properties"]["status"]["enum"]
            if "current" in status:
                self.current.append(item)
            if "finished" in status:
                self.finished.append(item)

    def set_up(self):
        '''do some setup for script'''

        if self.UPDATE:
            print("WARNING: This run is an " +
                  "UPDATE run objects will be released.")
        else:
            print("Object status will be checked but not changed")
        if self.FORCE:
            print("WARNING: Objects that do not " +
                  "pass audit will be FORCE-released")
        if self.LOGALL:
            print("Logging all statuses")
        if self.infile:
            if os.path.isfile(self.infile):
                self.ACCESSIONS = [line.rstrip('\n') for line in open(
                    self.infile)]
            else:
                self.ACCESSIONS = self.infile.split(",")
        elif self.QUERY:
            if "search" in self.QUERY:
                temp = encodedcc.get_ENCODE(
                    self.QUERY, self.connection).get("@graph", [])
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
            # if something happens and we end up with no accessions stop
            print("ERROR: object has no identifier", file=sys.stderr)
            sys.exit(1)

    def helper(self, item):
        '''feed this back to making references between official object name \
        and the name used in things like the @id
        such as Library and libraries'''
        if item.endswith("y"):
            # this is a hack to find words like "Library"
            item = item.rstrip("y") + "ies"
        elif item.endswith("s"):
            # check for things with "Series" type endings
            pass
        else:
            item = item + "s"
        return item.lower()

    def make_profile(self, dictionary, object_type):
        '''builds the PROFILES reference dictionary
        keysLink is the list of keys that point to links,
        used in the PROFILES'''
        d = dictionary["properties"]
        for prop in d.keys():
            if (object_type == 'Dataset' and
                prop not in ['files', 'contributing_files']) or \
               (object_type == 'File' and
                prop != 'derived_from') or \
               (object_type not in ['Dataset', 'File']):

                if d[prop].get("linkTo"):
                    self.keysLink.append(prop)
                else:
                    if d[prop].get("items"):
                        if (object_type == 'File' and prop == 'quality_metrics') or \
                           (object_type == 'Dataset' and
                           prop in ['replicates', 'original_files']):
                            i = d[prop].get("items")
                            if i.get("linkFrom"):
                                self.keysLink.append(prop)
                        else:
                            i = d[prop].get("items")
                            if i.get("linkTo"):
                                self.keysLink.append(prop)

    def process_link(self, identifier_link, approved_types):
        # print ("entering process_link with " + identifier_link)
        #print ('Replicate' in approved_types)
        item = identifier_link.split("/")[1].replace("-", "")
        subobj = encodedcc.get_ENCODE(identifier_link, self.connection)
        subobjname = subobj["@type"][0]
        restricted_flag = False
        if (subobjname == 'File') and (self.is_restricted(subobj) is True):
            print (subobj['@id'] + ' is restricted, ' +
                   'therefore will not be released')
            restricted_flag = True
        if (item in self.profiles_ref) and \
           (identifier_link not in self.searched):
            # expand subobject
            if (subobjname in approved_types) and \
               (restricted_flag is False):
                self.get_status(
                    subobj,
                    hi.dictionary_of_lower_levels.get(
                        hi.levels_mapping.get(subobjname)))

    def update_self(self, subobj, subobjname, update_status_flag):
        self.searched.append(subobj["@id"])
        if update_status_flag is True:
            self.statusDict[subobj["@id"]] = [subobjname,
                                              subobj["status"]]

    def is_restricted(self, file_object):
        if 'restricted' in file_object and file_object['restricted'] is True:
            return True
        return False

    def get_status(self, obj, approved_for_update_types):
        '''take object get status, @type, @id, uuid
        {@id : [@type, status]}'''
        name = obj["@type"][0]
        self.searched.append(obj["@id"])

        if self.PROFILES.get(name):
            self.statusDict[obj["@id"]] = [name, obj["status"]]
            # print (obj.keys())
            for key in obj.keys():
                # loop through object properties
                if key in self.PROFILES[name]:
                    # if the key is in profiles it's a link
                    if type(obj[key]) is list:
                        for link in obj[key]:
                            self.process_link(
                                link, approved_for_update_types)
                    else:
                        self.process_link(
                            obj[key], approved_for_update_types)

    def releasinator(self, name, identifier, status):
        '''releases objects into their equivalent released states'''
        patch_dict = {}
        if name in self.current:
            log = '%s' % "UPDATING: {} {} with status {} ".format(
                name, identifier, status) + \
                "is now current"
            patch_dict = {"status": "current"}
        elif name in self.finished:
            log = '%s' % "UPDATING: {} {} with status {} ".format(
                name, identifier, status) + \
                "is now finished"
            patch_dict = {"status": "finished"}
        else:
            log = "UPDATING: {} {} with status {} ".format(
                name, identifier, status) + \
                "is now released"
            patch_dict = {"status": "released"}
        if name in self.date_released:
            # if the object would have a date_released give it one
            now = datetime.datetime.now().date()
            patch_dict = {"date_released": str(now), "status": "released"}
            log += " with date {}".format(now)
        logger.info('%s' % log)
        if self.PRINTALL:
            print (log)
        encodedcc.patch_ENCODE(identifier, self.connection, patch_dict)

    def run_script(self):
        # set_up() gets all the command line arguments and validates them
        # also makes the list of accessions to run from
        t0 = time.time()
        self.set_up()

        good = ["released",
                "current",
                "disabled",
                "published",
                "finished",
                "virtual"]

        bad = ["replaced",
               "revoked",
               "deleted",
               "upload failed",
               "archived",
               "format check failed",
               "content error",
               "uploading",
               "error"]

        ignore = ["User",
                  "AntibodyCharacterization",
                  "Publication"]
        print ("Releasenator version " + str(self.releasenator_version))
        for accession in self.ACCESSIONS:
            print ("Processing accession: " + accession)
            self.searched = []
            expandedDict = encodedcc.get_ENCODE(accession, self.connection)
            objectStatus = expandedDict.get("status")
            obj = expandedDict["@type"][0]

            audit = encodedcc.get_ENCODE(accession, self.connection,
                                         "page").get("audit", {})
            passAudit = True
            logger.info('Releasenator version ' + str(self.releasenator_version))
            logger.info('%s' % "{}: {} Status: {}".format(obj, accession,
                        objectStatus))
            if audit.get("ERROR", ""):
                logger.warning('%s' % "WARNING: Audit status: ERROR")
                passAudit = False
            if audit.get("NOT_COMPLIANT", ""):
                logger.warning('%s' % "WARNING: Audit status: NOT COMPLIANT")
                passAudit = False
            self.statusDict = {}

            self.get_status(
                expandedDict,
                hi.dictionary_of_lower_levels.get(
                    hi.levels_mapping.get(obj)))
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
                            log = '%s' % "{} has status {}".format(
                                key, status)
                            logger.info(log)
                            # print (log)
                    elif status in bad:
                        log = '%s' % "WARNING: {} ".format(key) + "has status {}".format(status)
                        # print (log)
                        logger.warning(log)
                    else:
                        log = '%s' % "{} has status {}".format(
                            key, status)
                        # print (log)
                        logger.info(log)
                        if self.UPDATE:
                            if passAudit:
                                self.releasinator(name, key, status)
                    named.append(name)
        print("Data written to file", self.outfile)
        if self.TIMING:
            timing = int(time.time() - t0)
            print ("Execution of releasenator script took " + str(timing) + " seconds")

def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print ("Running on", key.server)
    # build the PROFILES reference dictionary
    release = Data_Release(args, connection)
    release.run_script()

if __name__ == "__main__":
    main()
