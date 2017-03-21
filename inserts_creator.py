import argparse
import os
import encodedcc
import logging
import sys
import urllib.request
import json
import re

EPILOG = '''
%(prog)s is a script that will extract uuid(s) of all the objects
that are linked to the dataset accession given as input


Basic Useage:

    %(prog)s --infile file.txt with uuids


Misc. Useage:

    The output file default is 'Extract_report.txt'
    This can be changed with '--output'

    Default keyfile location is '~/keyfile.json'
    Change with '--keyfile'

    Default key is 'default'
    Change with '--key'

'''

logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--infile',
                        help="File containing uuids")
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('~/keypairs.json'))

    args = parser.parse_args()
    logging.getLogger("requests").setLevel(logging.WARNING)

    return args


def convert_links(link_to, reg_ex):
    if isinstance(link_to, str):
        if reg_ex.match(str(link_to)) is not None:
            return link_to.split('/')[2]
        else:
            return link_to
    elif isinstance(link_to, list):
        to_return_list = []
        for entry in link_to:
            to_return_list.append(convert_links(entry, reg_ex))
        return to_return_list
    elif isinstance(link_to, dict):
        for k in link_to.keys():
            link_to[k] = convert_links(link_to[k], reg_ex)
        return link_to
    return link_to


def make_profile(dictionary, object_type):
    '''builds the PROFILES reference dictionary
    keysLink is the list of keys that point to links,
    used in the PROFILES'''
    to_return = []
    d = dictionary["properties"]
    for prop in d.keys():
            if d[prop].get("linkFrom"):
                to_return.append(prop)
            else:
                if d[prop].get("items"):
                    i = d[prop].get("items")
                    if i.get("linkFrom"):
                        to_return.append(prop)
    return to_return


def create_inserts(args, connection):
    PROFILES = {}
    temp = encodedcc.get_ENCODE("/profiles/", connection)
    profilesJSON = []
    for profile in temp.keys():
        profilesJSON.append(profile)

    for item in profilesJSON:
        profile = temp[item]
        object_type = item
        PROFILES[item] = make_profile(profile, object_type)

    link_to_regex = re.compile('^/.+/.+/$')

    if args.infile:
        if os.path.isfile(args.infile):
            uuids = [(line.rstrip('\n').split()[0],line.rstrip('\n').split()[1]) for line in open(
                args.infile)]
    if len(uuids) == 0:
        # if something happens and we end up with no accessions stop
        print("ERROR: object has no identifier")
        sys.exit(1)
    files_dict = {}
    strings_dict = {}
    for (obj_type, uuid) in sorted(uuids):
        if obj_type not in files_dict:
            file_name = re.sub('(?<!^)(?=[A-Z])', '_', obj_type).lower()
            files_dict[obj_type] = open('inserts/' + file_name + '.json', 'w')
            strings_dict[obj_type] = []
    new_object_dict = {}
    for (obj_type, uuid) in sorted(uuids):
        object_dict = encodedcc.get_ENCODE(uuid, connection, frame='edit')
        new_object_dict['uuid'] = uuid
        for key in object_dict.keys():
            if key not in PROFILES[obj_type]:
                new_object_dict[key] = convert_links(object_dict[key],
                                                     link_to_regex)
        if 'attachment' in object_dict:
            try:
                urllib.request.urlretrieve("https://www.encodeproject.org/" +
                                           uuid +
                                           '/' +
                                           object_dict['attachment']['href'],
                                           'documents/' +
                                           object_dict[
                                               'attachment']['download'])
            except urllib.error.HTTPError:
                print ('non exsting attachment?')
            else:
                new_object_dict['attachment'] = object_dict[
                    'attachment']['download']
        x = json.dumps(new_object_dict, indent=4, sort_keys=True)
        strings_dict[obj_type].append(x)
        new_object_dict = {}

    for obj_type in strings_dict.keys():
        final_output = '['
        for x in strings_dict[obj_type]:
            final_output += '\n' + x + ','
        final_output = final_output[:-1] + '\n]'
        files_dict[obj_type].write(final_output)
        files_dict[obj_type].flush()


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    create_inserts(args, connection)


if __name__ == "__main__":
    main()
