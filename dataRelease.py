# release script
import requests
import argparse
import os
import json
import re
import datetime
from urllib.parse import urljoin
global AUTHID, AUTHPW, SERVER, HEADERS, DEBUG, PROFILES, profilesJSON, ACCESSIONS
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


def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', help="File containing accessions")
    parser.add_argument('--accession', help="Accession number for experiment")
    parser.add_argument('--outfile', help="Output file name", default='output.txt')
    parser.add_argument('--key', help="The keypair identifier from the keyfile.", default='default')
    parser.add_argument('--keyfile', help="The keyfile", default=os.path.expanduser('./keypairs.json'))
    parser.add_argument('--debug', help="Debug prints out HTML requests and returned JSON objects. Default is off", action='store_true', default=False)
    parser.add_argument('--update', help="Run script and update the objects. Default is off", action='store_true', default=False)
    parser.add_argument('--query', help="Custom query for accessions")
    parser.add_argument('--force', help="Forces release of experiments that did not pass audit. Default is off", action='store_true', default=False)
    parser.add_argument('--logall', help="Adds status of 'released' objects to output file. Default is off", action='store_true', default=False)
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


def doRequest(uri, audit=False):
    '''GET an ENCODE object as JSON and return as dict'''
    if audit:
        uri += '?frame=page'
    else:
        if 'search' in uri:
            uri += '&limit=all&frame=object'
        elif 'frame' not in uri:
            uri += "?frame=object"
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


def regexCheck(reg, str):
    try:
        find = re.match(reg, str).group(1)
    except:
        find = None
    return find


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


def expandLinks(dictionary):
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
                        temp = doRequest(link)
                        newLinkValues.append(temp)
                        expandLinks(temp)
                d[key] = newLinkValues
            elif type(d.get(key)) is str:
                if d.get(key) not in searched:
                    temp = doRequest(d.get(key))
                    d[key] = temp
                    expandLinks(temp)


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


def releasinator(name, uuid, status):
    '''releases objects into their equivalent released states'''
    url = urljoin(SERVER, uuid)
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
    json_data = json.dumps(stats)
    # patch new status into object
    if DEBUG:
        print("DEBUG: PATCH %s" % (url))
    response = requests.patch(url, auth=(AUTHID, AUTHPW), data=json_data, headers=HEADERS)
    if DEBUG:
        print("DEBUG: PATCH RESPONSE code %s" % (response.status_code))
        try:
            if response.json():
                print("DEBUG: PATCH RESPONSE JSON")
                print(json.dumps(response.json(), indent=4, separators=(',', ': ')))
        except:
            print("DEBUG: PATCH RESPONSE text %s" % (response.text))
    if not response.status_code == 200:
        print(response.text)


args = getArgs()
AUTHID, AUTHPW, SERVER = processkey(args.key, args.keyfile)
DEBUG = args.debug
infile = args.infile
outfile = args.outfile
singleAcc = args.accession
UPDATE = args.update
QUERY = args.query
FORCE = args.force
LOGALL = args.logall
print("Running on", SERVER)
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
    profile = doRequest("profiles/" + item + ".json")
    keysChecked = []
    keysLink = []  # if a key is in this list, it points to a link and will be embedded in the final product
    makeProfile(profile)
    PROFILES[item] = keysLink
if singleAcc:
    ACCESSIONS = [singleAcc]
    outfile = singleAcc
elif QUERY:
    graph = doRequest(QUERY)
    for exp in graph.get("@graph", []):
        ACCESSIONS.append(exp.get("accession"))
elif infile:
    ACCESSIONS = [line.rstrip('\n') for line in open(infile)]

good = ["released", "current", "disabled", "published", "finished", "virtual"]
bad = ["replaced", "revoked", "deleted", "upload failed", "format check failed", "uploading", "error"]
ignore = ["users", "antibody-characterizations", "publications"]

with open(outfile, "w") as f:
    for accession in ACCESSIONS:
        f.write(accession + "\n")
        searched = []
        expandedDict = doRequest(accession)
        expandLinks(expandedDict)
        experimentStatus = expandedDict.get("status")
        audit = doRequest(accession, True).get("audit")
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
            name = regexCheck('/(.+?)/', key)
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
                            releasinator(name, uuid, status)
                named.append(name)
        f.write("\n\n\n\n")
        if UPDATE:
            if passAudit:
                url = urljoin(SERVER, accession)
                now = datetime.datetime.now().date()
                json_data = json.dumps({"date_released": str(now)})
                requests.patch(url, auth=(AUTHID, AUTHPW), data=json_data, headers=HEADERS)
print("Data written to file", outfile)
