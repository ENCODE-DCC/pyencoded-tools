import argparse
import os.path
import encodedcc
from dateutil.parser import parse

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--infile',
                        help="the infile",
                        default="duplicates_global.txt")
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


def updater(status_dict, accessions, md5sum, experiments, connection, update):
    newest, exp = status_dict[max(status_dict.keys())]
    accessions.remove(newest)
    patch_dict = {"alternate_accessions": accessions}
    for a in accessions:
        print("patching {} with status replaced".format(a))
        if update:
            encodedcc.patch_ENCODE(a, connection, {"status": "replaced"})
    print("{md5sum} {newfile} {fileexp} {otherfiles} {otherexp}".format(md5sum=md5sum, newfile=newest, fileexp=exp, otherfiles=accessions, otherexp=experiments))
    if update:
        encodedcc.patch_ENCODE(newest, connection, patch_dict)


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    headers = ["accession", "experiment", "md5sum", "file_type", "status"]
    data = []
    with open(args.infile, "r") as infile:
        for line in infile:
            if line.startswith("LAB"):
                pass
            elif line.startswith("NUM"):
                pass
            else:
                values = line.rstrip("\n").split("\t")
                temp = dict(zip(headers, values))
                print("Getting date created for", temp["accession"])
                date = encodedcc.get_ENCODE(temp["accession"], connection)
                if date.get("date_created"):
                    temp["date_created"] = parse(date["date_created"])
                    data.append(temp)
    sums = {}
    for d in data:
        sums[d["md5sum"]] = []
    for s in sums.keys():
        for d in data:
            if d["md5sum"] == s:
                sums[s].append(d)

    good = ["released", "in progress"]
    bad = ["deleted", "revoked"]
    for s in sums.keys():
        status = []
        accessions = []
        experiments = []
        active = {}
        deprecate = {}
        for d in sums[s]:
            status.append(d["status"])
            accessions.append(d["accession"])
            experiments.append(d["experiment"])
            if d["status"] in good:
                active[d["date_created"]] = [d["accession"], d["experiment"]]
            elif d["status"] in bad:
                deprecate[d["date_created"]] = [d["accession"], d["experiment"]]
            else:
                print("Error: status {} was not expected".format(d["status"]))
        if len(active.keys()) > 1:
            print("ERROR: Too many items in 'active' state! {} items found".format(len(active.keys())))
        elif len(active.keys()) == 1 and len(deprecate.keys()) > 0:
            #print("replace deprecated with active")
            updater(active, accessions, s, experiments, connection, args.update)
        elif len(active.keys()) == 0 and len(deprecate.keys()) > 0:
            #print("replaced oldest deprecate with newest")
            updater(deprecate, accessions, s, experiments, connection, args.update)


if __name__ == '__main__':
        main()
