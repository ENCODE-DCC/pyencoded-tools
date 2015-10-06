#!/usr/bin/env python
# -*- coding: latin-1 -*-
import argparse
import os
import re
import datetime
import encodedcc
import logging

EPILOG = '''takes a file with a list of experiment accessions, a query,
or a single accession and checks all associated objects
and sets them to their released status'''
logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser(epilog=EPILOG)
    parser.add_argument('--infile',
                        help="File containing accessions")
    parser.add_argument('--accession',
                        help="Accession number for experiment")
    parser.add_argument('--outfile',
                        help="Output file name", default='output.txt')
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('./keypairs.json'))
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
    if args.accession:
        filename = args.accession
    else:
        filename = args.outfile
    if args.debug:
        logging.basicConfig(filename=filename, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.DEBUG)
    else:  # use the default logging level
        logging.basicConfig(filename=filename, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.INFO)
    return args


class Data_Release():
    def __init__(self, args):
        self.infile = args.infile
        self.outfile = args.outfile
        self.singleAcc = args.accession
        self.QUERY = args.query
        self.LOGALL = args.logall
        self.FORCE = args.force
        self.UPDATE = args.update
        self.keysChecked = []
        self.keysLink = []
        self.PROFILES = {}
        self.ACCESSIONS = []
        self.statusDict = {}
        self.profilesJSON = ["experiment", "biosample", "file", "replicate",
                             "lab", "target", "award", "rnai", "talen",
                             "library", "mouse_donor", "human_donor",
                             "construct", "dataset", "antibody_lot",
                             "antibody_approval", "fly_donor", "worm_donor",
                             "rnai_characterization", "bismark_quality_metric",
                             "bigwigcorrelate_quality_metric",
                             "chipseq_filter_quality_metric",
                             "dnase_peak_quality_metric",
                             "edwbamstats_quality_metric",
                             "edwcomparepeaks_quality_metric",
                             "encode2_chipseq_quality_metric",
                             "fastqc_quality_metric", "quality_metric",
                             "hotspot_quality_metric", "star_quality_metric",
                             "idr_summary_quality_metric",
                             "mad_quality_metric", "pbc_quality_metric",
                             "phantompeaktools_spp_quality_metric",
                             "samtools_flagstats_quality_metric",
                             "samtools_stats_quality_metric",
                             "analysis_step_version"]

    def make_profile(self, dictionary):
        '''builds the PROFILES reference dictionary
        keysLink is the list of keys that point to links, used in the PROFILES'''
        d = dictionary
        if d.get("linkTo") or d.get("linkFrom"):
            if self.keysChecked[-1] == "items":
                self.keysLink.append(self.keysChecked[-2])
            else:
                self.keysLink.append(self.keysChecked[-1])
        else:
            for key in d.keys():
                if type(d.get(key)) is dict:
                    self.keysChecked.append(key)  # check a key, no check on second pass
                    self.make_profile(d.get(key))  # should prevent infinite loops

    def expand_links(self, dictionary, connection):
        '''uses PROFILES to build the expanded instance of the object fed in'''
        d = dictionary
        if d.get("@id"):
            self.searched.append(d.get("@id"))
        for key in d.keys():
            name = d.get("@type")[0]
            if key in self.PROFILES.get(name, []):
                newLinkValues = []
                if type(d.get(key)) is list:
                    for link in d.get(key):  # loop through keys
                        if link not in self.searched:  # if we have not checked this link yet we continue
                            temp = encodedcc.get_ENCODE(link, connection)
                            newLinkValues.append(temp)
                            self.expand_links(temp, connection)
                    d[key] = newLinkValues
                elif type(d.get(key)) is str:
                    if d.get(key) not in self.searched:
                        temp = encodedcc.get_ENCODE(d.get(key), connection)
                        d[key] = temp
                        self.expand_links(temp, connection)

    def find_status(self, dictionary):
        '''takes the object created by expand_links and makes a new dict,
        with each subobject and its status {@id : [uuid, status]}'''
        d = dictionary
        name = d.get("@type")[0]
        self.statusDict[d.get("@id")] = [d.get("uuid"), d.get("status")]
        for key in d.keys():
            if key in self.PROFILES.get(name, []):
                if type(d.get(key, [])) is list:
                    for item in d.get(key, []):
                        self.find_status(item)
                elif type(d.get(key)) is dict:
                    self.find_status(d.get(key))

    def releasinator(self, name, uuid, status, connection):
        '''releases objects into their equivalent released states'''
        status_current = ["targets", "treatments", "awards", "organisms", "platforms"]
        status_finished = ["analysis-step-run"]
        stats = {}
        if name in status_current:
            log = "UPDATING: " + name + " " + uuid + " with status " + status + " is now current\n"
            logger.info('%s' % log)
            stats = {"status": "current"}
        elif name in status_finished:
            log = "UPDATING: " + name + " " + uuid + " with status " + status + " is now finished\n"
            logger.info('%s' % log)
            stats = {"status": "finished"}
        else:
            log = "UPDATING: " + name + " " + uuid + " with status " + status + " is now released\n"
            logger.info('%s' % log)
            stats = {"status": "released"}
        encodedcc.patch_ENCODE(uuid, connection, stats)

    def run_script(self, connection):
        if self.UPDATE:
            print("WARNING: This run is an UPDATE run objects will be released.")
        if self.FORCE:
            print("WARNING: Objects that do not pass audit will be FORCE-released")
        else:
            print("Object status will be checked but not changed")
        if self.LOGALL:
            print("Logging all statuses")
        for item in self.profilesJSON:
            profile = encodedcc.get_ENCODE("profiles/" + item + ".json", connection)
            self.keysChecked = []
            self.keysLink = []  # if a key is in this list, it points to a link and will be embedded in the final product
            self.make_profile(profile)
            self.PROFILES[item] = self.keysLink
        if self.singleAcc:
            self.ACCESSIONS = [self.singleAcc]
        elif self.QUERY:
            graph = encodedcc.get_ENCODE(self.QUERY, connection)
            for exp in graph.get("@graph", []):
                self.ACCESSIONS.append(exp.get("accession"))
        elif self.infile:
            self.ACCESSIONS = [line.rstrip('\n') for line in open(self.infile)]
        if self.singleAcc:
            filename = self.singleAcc
        else:
            filename = self.outfile

        good = ["released", "current", "disabled", "published", "finished", "virtual"]
        bad = ["replaced", "revoked", "deleted", "upload failed",
               "format check failed", "uploading", "error"]
        ignore = ["users", "antibody-characterizations", "publications"]
        for accession in self.ACCESSIONS:
            log = accession + "\n"
            logger.info('%s' % log)
            self.searched = []
            expandedDict = encodedcc.get_ENCODE(accession, connection)
            self.expand_links(expandedDict, connection)
            experimentStatus = expandedDict.get("status")
            audit = encodedcc.get_ENCODE(accession, connection, "page").get("audit", {})
            passAudit = True
            log = "Experiment Status:" + experimentStatus
            logger.info('%s' % log)
            if audit.get("ERROR", ""):
                log = "Audit status: ERROR"
                logger.warning('%s' % log)
                passAudit = False
            if audit.get("NOT_COMPLIANT", ""):
                log = "Audit status: NOT COMPLIANT"
                logger.warning('%s' % log)
                passAudit = False
            self.statusDict = {}
            self.find_status(expandedDict)
            if self.FORCE:
                passAudit = True
            named = []
            for key in sorted(self.statusDict.keys()):
                status = str(self.statusDict.get(key, [])[1])
                uuid = str(self.statusDict.get(key, [])[0])
                try:
                    name = re.match('/(.+?)/', key).group(1)
                except:
                    name = None
                if name not in ignore:
                    if name not in named:
                        log = name.upper()
                        logger.info('%s' % log)
                    if status in good:
                        if self.LOGALL:
                            log = name + " " + uuid + " has status " + status
                            logger.info('%s' % log)
                    elif status in bad:
                        log = name + " " + uuid + " has status " + status
                        logger.warning('%s' % log)
                    else:
                        log = name + " " + uuid + " has status " + status
                        logger.info('%s' % log)
                        if self.UPDATE:
                            if passAudit:
                                self.releasinator(name, uuid, status, connection)
                    named.append(name)
            if self.UPDATE:
                if passAudit:
                    now = datetime.datetime.now().date()
                    json_data = {"date_released": str(now)}
                    encodedcc.patch_ENCODE(accession, connection, json_data)
        print("Data written to file", filename)


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on", key.server)
    # build the PROFILES reference dictionary
    release = Data_Release(args)
    release.run_script(connection)

if __name__ == "__main__":
    main()
