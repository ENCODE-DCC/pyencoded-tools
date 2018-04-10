#!/usr/bin/env python3
# -*- coding: latin-1 -*-


'''
Releasenator changelog

Version 1.4

Blocks release of objects associated with HeLa data unless --hela flag
specified.

Version 1.3

Releasenator will no longer release files that are associated with pipelines
that are not in "released" status, for such files warning will be printed.
Released earlier files will remain released, even if they are associated with
unreleased pipelines, the script will print out warning messages for these
files as well.

'''
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


def make_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


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
                        help="Output file name",
                        default='{}_{}.txt'.format('release_report', time.strftime("%Y_%b_%d_%I_%M_%S")))
    parser.add_argument('--out_dir',
                         help='Directory to store release reports', default='release_reports')
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
    parser.add_argument('--hela',
                        help='Force release of HeLa data.',
                        action='store_true')
    args = parser.parse_args()
    make_dir(args.out_dir)
    run_type = 'update' if args.update else 'dry_run'
    args.outfile = '{}/{}_{}'.format(args.out_dir, run_type, args.outfile)
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
        self.releasenator_version = 1.4
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
        self.searched = []
        self.connection = connection
        self.HELA = args.hela
        # Define objects associated with HeLa data.
        self.hela_associated_objects = [
            'Experiment',
            'Biosample',
            'File',
            'Library',
            'Replicate'
        ]
        temp = encodedcc.get_ENCODE("/profiles/", self.connection)
        # Fix for WRAN-708, new objects in profiles that don't have properties.
        temp = {k: v for k, v in temp.items() if isinstance(v, dict) and v.get('properties')}
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
        if self.HELA:
            print('WARNING: Objects associated with HeLa data will be released')
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
        item = identifier_link.split("/")[1].replace("-", "")
        subobj = encodedcc.get_ENCODE(identifier_link, self.connection)
        subobjname = subobj["@type"][0]
        restricted_flag = False
        inactive_pipeline_flag = False

        if (item in self.profiles_ref) and \
           (identifier_link not in self.searched):
            if (subobjname == 'File'):
                if self.is_restricted(subobj) is True:
                    print(subobj['@id'] + ' is restricted, ' +
                          'therefore will not be released')
                    restricted_flag = True
                    self.searched.append(subobj["@id"])
                if subobj.get('analysis_step_version'):
                    p = self.has_inactive_pipeline(encodedcc.get_ENCODE(
                        identifier_link,
                        self.connection, "embedded"))
                    if p:
                        print('{} is only associated with inactive pipelines'
                              ' and therefore will not be released: {}'.format(subobj['@id'], p))
                        inactive_pipeline_flag = True
                        self.searched.append(subobj["@id"])
            # expand subobject
            if (subobjname in approved_types) and \
               (restricted_flag is False) and \
               (inactive_pipeline_flag is False):
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

    def has_inactive_pipeline(self, file_object):
        pipelines = file_object.get('analysis_step_version',
                                    {}).get('analysis_step',
                                            {}).get('pipelines')
        if pipelines is not None:
            if all([p['status'] != 'released' for p in pipelines]):
                return [p['@id'] for p in pipelines]
        return False

    def _get_associated_term_id(self, data_type, data):
        """
        Find biosample_term_id associated with particular object.
        """
        obj_id = None
        if data_type == 'File':
            # Get biosample_term_id in file.dataset.
            obj_id = data.get('dataset')
        elif data_type == 'Replicate':
            # Get biosample_term_id in replicate.experiment.
            obj_id = data.get('experiment')
        elif data_type == 'Library':
            # Get biosample_term_id in library.biosample.
            obj_id = data.get('biosample')
        else:
            # For experiments and biosamples.
            biosample_term_id = data.get('biosample_term_id')
        if obj_id is not None:
            # Return biosample_term_id of embedded object.
            biosample_term_id = encodedcc.get_ENCODE(
                obj_id,
                self.connection
            ).get('biosample_term_id')
        return biosample_term_id

    def associated_with_hela_data(self, data_type, data):
        """
        Block HeLa data if found.
        """
        if data_type in self.hela_associated_objects:
            biosample_term_id = self._get_associated_term_id(data_type, data)
            # Assumes EFO:000279 includes all HeLa data.
            if biosample_term_id == 'EFO:0002791':
                # Message to screen.
                print('WARNING: Release of HeLa data prohibited unless '
                      'explicitly approved by NHGRI. {} will not be released unless '
                      '--hela flag specified. SKIPPING!'.format(data['@id']))
                # Message to log.
                logger.warning('WARNING: Trying to release HeLa '
                               'data: {}. SKIPPING!'.format(data['@id']))
                # Will prevent release of object.
                return True
        return False

    def has_audit(self, accession):
        # Another GET request for page frame.
        audit = encodedcc.get_ENCODE(accession,
                                     self.connection,
                                     'page').get('audit', {})
        if (audit.get('ERROR') is not None
                or audit.get('NOT_COMPLIANT') is not None):
            details = [v[0]['category'] for v in audit.values()]
            message = 'WARNING: AUDIT on object: {}. SKIPPING!'.format(details)
            print(message)
            logger.warning(message)
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
            print(log)
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
        version = 'Releasenator version {}'.format(self.releasenator_version)
        print(version)
        logger.info(version)
        for accession in self.ACCESSIONS:
            print('Processing accession:', accession)
            data = encodedcc.get_ENCODE(accession, self.connection)
            data_status = data.get('status')
            data_type = data['@type'][0]
            logger.info('{}: {} Status: {}'.format(data_type,
                                                   accession,
                                                   data_status))
            # Skip if associated with HeLa data.
            if not self.HELA and self.associated_with_hela_data(data_type, data):
                continue
            # Skip if has audit.
            if not self.FORCE and self.has_audit(accession):
                continue
            self.statusDict = {}
            self.get_status(
                data,
                hi.dictionary_of_lower_levels.get(
                    hi.levels_mapping.get(data_type)))
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
                        log = '%s' % "WARNING: {} ".format(
                            key) + "has status {}".format(status)
                        # print (log)
                        logger.warning(log)
                    else:
                        log = '%s' % "{} has status {}".format(
                            key, status)
                        # print (log)
                        logger.info(log)
                        if self.UPDATE:
                            self.releasinator(name, key, status)
                    named.append(name)
        print("Data written to file", self.outfile)
        if self.TIMING:
            timing = int(time.time() - t0)
            print("Execution of releasenator script took " +
                  str(timing) + " seconds")


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
