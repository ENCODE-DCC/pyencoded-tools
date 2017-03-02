import argparse
import os
import encodedcc
import logging
import sys
import urllib.request
import json

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


def create_inserts(args, connection):
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
            files_dict[obj_type] = open('inserts/' + obj_type + '.json', 'w')
            strings_dict[obj_type] = []

    for (obj_type, uuid) in sorted(uuids):
        object_dict = encodedcc.get_ENCODE(uuid, connection, frame='edit')

        if 'attachment' in object_dict:
            urllib.request.urlretrieve("https://www.encodeproject.org/" +
                                       uuid +
                                       '/' + object_dict['attachment']['href'],
                                       'documents/' +
                                       object_dict['attachment']['download'])
            object_dict['attachment'] = object_dict['attachment']['download']
        x = json.dumps(object_dict, indent=4, sort_keys=True)
        strings_dict[obj_type].append(x)

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