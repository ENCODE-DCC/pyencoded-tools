#!/usr/bin/env python
# -*- coding: latin-1 -*-
import argparse
import os
import re
import datetime
import encodedcc

EPILOG = '''takes a file with a list of experiment accessions, a query,
or a single accession and checks all associated objects
and sets them to their released status'''

global DEBUG, PROFILES, profilesJSON, ACCESSIONS


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
    parser.add_argument('--debug',
                        help="Debug prints out HTML requests and returned JSON objects. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--update',
                        help="Run script and update the objects. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--query',
                        help="Custom query for accessions")
    parser.add_argument('--force',
                        help="Forces release of experiments that did not pass audit. Default is off",
                        action='store_true', default=False)
    parser.add_argument('--logall',
                        help="Adds status of 'released' objects to output file. Default is off",
                        action='store_true', default=False)
    args = parser.parse_args()
    return args


def makeProfile(dictionary):
    '''builds the PROFILES reference dictionary
    keysLink and keysChecked are tracked outside of method
    keysLink is the list of keys that point to links, used in the PROFILES'''
    d = dictionary
    if d.get("linkTo") or d.get("linkFrom"):
        if keysChecked[-1] == "items":
            keysLink.append(keysChecked[-2])
        else:
            keysLink.append(keysChecked[-1])
    else:
        for key in d.keys():
            if type(d.get(key)) is dict:
                keysChecked.append(key)  # once we check a key, we put it here so we know not to check on a second pass
                makeProfile(d.get(key))  # and hopefully prevent infinite loops


def expandLinks(dictionary, connection):
    '''uses PROFILES to build the expanded instance of the object fed in'''
    d = dictionary
    if d.get("@id"):
        searched.append(d.get("@id"))
    for key in d.keys():
        name = d.get("@type")[0]
        if key in PROFILES.get(name, []):
            newLinkValues = []
            if type(d.get(key)) is list:
                for link in d.get(key):  # loop through keys
                    if link not in searched:  # if we have not checked this link yet we continue
                        temp = encodedcc.get_ENCODE(link, connection)
                        newLinkValues.append(temp)
                        expandLinks(temp, connection)
                d[key] = newLinkValues
            elif type(d.get(key)) is str:
                if d.get(key) not in searched:
                    temp = encodedcc.get_ENCODE(d.get(key), connection)
                    d[key] = temp
                    expandLinks(temp, connection)


def findStatus(dictionary):
    '''takes the object created by expandLinks and makes a new dict,
    with each subobject and its status {@id : [uuid, status]} '''
    d = dictionary
    name = d.get("@type")[0]
    statusDict[d.get("@id")] = [d.get("uuid"), d.get("status")]
    for key in d.keys():
        if key in PROFILES.get(name, []):
            if type(d.get(key, [])) is list:
                for item in d.get(key, []):
                    findStatus(item)
            elif type(d.get(key)) is dict:
                findStatus(d.get(key))


def releasinator(name, uuid, status, connection):
    '''releases objects into their equivalent released states'''
    status_current = ["targets", "treatments", "awards", "organisms", "platforms"]
    status_finished = ["analysis-step-run"]
    stats = {}
    if name in status_current:
        f.write("UPDATING: " + name + " " + uuid + " with status " + status + " is now current\n")
        stats = {"status": "current"}
    elif name in status_finished:
        f.write("UPDATING: " + name + " " + uuid + " with status " + status + " is now finished\n")
        stats = {"status": "finished"}
    else:
        f.write("UPDATING: " + name + " " + uuid + " with status " + status + " is now released\n")
        stats = {"status": "released"}
    encodedcc.patch_ENCODE(uuid, connection, stats)


def main():

    HEADERS = {'content-type': 'application/json', 'accept': 'application/json'}
    PROFILES = {}
    ACCESSIONS = []
    profilesJSON = ["experiment", "biosample", "file", "replicate", "lab",
                    "target", "award", "rnai", "talen", "library", "mouse_donor",
                    "human_donor", "construct", "dataset", "antibody_lot",
                    "antibody_approval", "fly_donor", "worm_donor",
                    "rnai_characterization", "bismark_quality_metric",
                    "bigwigcorrelate_quality_metric",
                    "chipseq_filter_quality_metric", "dnase_peak_quality_metric",
                    "edwbamstats_quality_metric", "edwcomparepeaks_quality_metric",
                    "encode2_chipseq_quality_metric", "fastqc_quality_metric",
                    "hotspot_quality_metric", "idr_summary_quality_metric",
                    "mad_quality_metric", "pbc_quality_metric",
                    "phantompeaktools_spp_quality_metric", "quality_metric",
                    "samtools_flagstats_quality_metric",
                    "samtools_stats_quality_metric", "star_quality_metric",
                    "analysis_step_version"]

    args = getArgs()
    DEBUG = args.debug
    infile = args.infile
    outfile = args.outfile
    singleAcc = args.accession
    UPDATE = args.update
    QUERY = args.query
    FORCE = args.force
    LOGALL = args.logall
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    print("Running on", key.server)
    if UPDATE:
        print("WARNING: This run is an UPDATE run objects will be released.")
        if FORCE:
            print("WARNING: Objects that do not pass audit will be FORCE-released")
    else:
        print("Object status will be checked but not changed")
    if LOGALL:
        print("Logging all statuses")
    # build the PROFILES reference dictionary
    for item in profilesJSON:
        profile = encodedcc.get_ENCODE("profiles/" + item + ".json", connection)
        keysChecked = []
        keysLink = []  # if a key is in this list, it points to a link and will be embedded in the final product
        makeProfile(profile)
        PROFILES[item] = keysLink
    if singleAcc:
        ACCESSIONS = [singleAcc]
        outfile = singleAcc
    elif QUERY:
        graph = encodedcc.get_ENCODE(QUERY, connection)
        for exp in graph.get("@graph", []):
            ACCESSIONS.append(exp.get("accession"))
    elif infile:
        ACCESSIONS = [line.rstrip('\n') for line in open(infile)]

    good = ["released", "current", "disabled", "published", "finished", "virtual"]
    bad = ["replaced", "revoked", "deleted", "upload failed",
           "format check failed", "uploading", "error"]
    ignore = ["users", "antibody-characterizations", "publications"]

    with open(outfile, "w") as f:
        for accession in ACCESSIONS:
            f.write(accession + "\n")
            searched = []
            expandedDict = encodedcc.get_ENCODE(accession, connection)
            expandLinks(expandedDict, connection)
            experimentStatus = expandedDict.get("status")
            audit = encodedcc.get_ENCODE(accession + '?frame=page', connection).get("audit")
            passAudit = True
            f.write("Experiment Status:" + experimentStatus + "\n")
            if audit.get("ERROR"):
                f.write("Audit status: ERROR\n")
                passAudit = False
            if audit.get("NOT_COMPLIANT"):
                f.write("Audit status: NOT COMPLIANT\n")
                passAudit = False
            statusDict = {}
            findStatus(expandedDict)
            if FORCE:
                passAudit = True
            named = []
            for key in sorted(statusDict.keys()):
                status = str(statusDict.get(key, [])[1])
                uuid = str(statusDict.get(key, [])[0])
                try:
                    name = re.match('/(.+?)/', key).group(1)
                except:
                    name = None
                if name not in ignore:
                    if name not in named:
                        f.write(name.upper() + "\n")
                    if status in good:
                        if LOGALL:
                            f.write("LOG: " + name + " " + uuid + " has status " + status + "\n")
                    elif status in bad:
                        f.write("WARNING: " + name + " " + uuid + " has status " + status + "\n")
                    else:
                        f.write(name + " " + uuid + " has status " + status + "\n")
                        if UPDATE:
                            if passAudit:
                                releasinator(name, uuid, status, connection)
                    named.append(name)
            f.write("\n\n\n\n")
            if UPDATE:
                if passAudit:
                    now = datetime.datetime.now().date()
                    encodedcc.patch_ENCODE(accession, connection, now)
    print("Data written to file", outfile)

if __name__ == "__main__":
    main()
