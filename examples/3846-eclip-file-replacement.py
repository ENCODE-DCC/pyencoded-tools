import argparse
import os.path
import encodedcc
import sys
from urllib.parse import quote

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--object',
                        help="Either the file containing a list of ENCs as a column\
                        or this can be a single accession by itself")
    parser.add_argument('--query',
                        help="A custom query to get accessions.")
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


def replacer(file, connection, update):
    if file.get("aliases"):
        # this has aliases
        if file["aliases"][0].endswith("_replaced"):
            # this is one of the old ones
            alias = file["aliases"][0].rstrip("_replaced")
            old_acc = file["accession"]
            old_date = file["date_created"]
            print(old_acc)
            new = encodedcc.get_ENCODE(quote(alias), connection)
            new_acc = new["accession"]
            new_date = new["date_created"]
            patch_dict = {"status": "replaced", "alternate_accessions": [alias]}
            #print("file {} with date {} replaces file {} with date {}".format(new_acc, new_date, old_acc, old_date))
            if update:
                encodedcc.patch_ENCODE(file["@id"], connection, patch_dict)
    else:
        print("file {} has no aliases".format(file["@id"]))


def renamer(file, connection, update):
    patch_dict = {}
    aliases = file.get("aliases", [])
    submitted = file.get("submitted_file_name", "").rstrip("_rm")
    submitted = submitted + "_rm"
    patch_dict["submitted_file_name"] = submitted
    if any(aliases):
        alias = aliases[0].rstrip("_replaced")
        alias = [alias + "_replaced"]
        patch_dict["aliases"] = alias
    else:
        print("skipping {} with no aliases".format(file["@id"]))
    print("file {} with data {}".format(file["@id"], patch_dict))
    if update:
        encodedcc.patch_ENCODE(file["@id"], connection, patch_dict)


def dict_maker(file, dictionary):
    file_type = file["file_type"]
    replicate = file["replicate"]
    run_type = file.get("run_type")
    paired_end = file.get("paired_end")
    date = file["date_created"]
    accession = file["accession"]
    temp = {"file_type": file_type, "replicate": replicate, "run_type": run_type, "paired_end": paired_end, "date": date}
    dictionary[accession] = temp


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on {}".format(connection.server))
    accessions = []
    if args.object:
        if os.path.isfile(args.object):
            accessions = [line.strip() for line in open(args.object)]
        else:
            accessions = args.object.split(",")
    elif args.query:
        if "search" in args.query:
            temp = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        else:
            temp = [encodedcc.get_ENCODE(args.query, connection)]
        if any(temp):
            for obj in temp:
                if obj.get("accession"):
                    accessions.append(obj["accession"])
                elif obj.get("uuid"):
                    accessions.append(obj["uuid"])
                elif obj.get("@id"):
                    accessions.append(obj["@id"])
                else:
                    print("ERROR: object has no identifier", file=sys.stderr)
    if len(accessions) == 0:
        print("No accessions to check!", file=sys.stderr)
        sys.exit(1)
    for acc in accessions:
        files = encodedcc.get_ENCODE(acc, connection).get("original_files", [])
        new_files = {}
        old_files = {}
        for f in files:
            file = encodedcc.get_ENCODE(f, connection)
            #renamer(file, connection, args.update)
            #replacer(file, connection, args.update)
            if any(file.get("aliases", [])):
                # this has aliases
                if file["aliases"][0].endswith("_replaced"):
                    # this is one of the old ones
                    dict_maker(file, old_files)
                else:
                    # this is a new file
                    dict_maker(file, new_files)
            else:
                print("file {} has no aliases".format(file["@id"]))

        for new in new_files.keys():
            new_temp = new_files[new]
            for old in old_files.keys():
                old_temp = old_files[old]

                if new_temp["replicate"] == old_temp["replicate"]:
                    #print(new_temp["replicate"], old_temp["replicate"])

                    if new_temp["file_type"] == old_temp["file_type"]:
                        #print(new_temp["file_type"], old_temp["file_type"])

                        if new_temp["run_type"] == old_temp["run_type"]:
                            #print(new_temp["run_type"], old_temp["run_type"])

                            if new_temp["paired_end"] == old_temp["paired_end"]:
                                #print(new_temp["paired_end"], old_temp["paired_end"])
                                print("New file {} with date {} replacing old file {} with date {}".format(new, new_temp["date"], old, old_temp["date"]))
                                if args.update:
                                    #replace old file
                                    encodedcc.patch_ENCODE(old, connection, {"status": "replaced"})
                                    # release and update new file
                                    patch_dict = {"status": "released", "alternate_accessions": [old]}
                                    encodedcc.patch_ENCODE(new, connection, patch_dict)


if __name__ == '__main__':
        main()
