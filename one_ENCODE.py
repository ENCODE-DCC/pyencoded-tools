#!/usr/bin/env python
# -*- coding: latin-1 -*-
''' Script to add one ENCODE object from a file or stdin or get one object from an ENCODE server
'''
import json
import sys
import os.path
from base64 import b64encode
#import magic
import mimetypes
import encodedcc
import re


EPILOG = '''Examples:

To get one ENCODE object from the server/keypair called "default" in the default keypair file and print the JSON:

    %(prog)s --id ENCBS000AAA

To use a different key from the default keypair file:

    %(prog)s --id ENCBS000AAA --key submit

To save the output:

    %(prog)s --id ENCBS000AAA --key submit > my_saved_json.json

To PATCH or POST from JSON:

    %(prog)s --infile my_new_json.json
    Where the file contains a @id property that, if it matches an existing object do a PATCH, else do a POST.

To force a PUT:

    %(prog)s --infile my_new_json.json --force-put

To force a GET only (no PATCH, PUT or POST) of the object as it exists in the database:

    %(prog)s --infile my_new_json.json --get-only

In case of emergency, break glass:

    echo '{"@id": "/biosamples/ENCBS999JSS/", "note": "This is destructive"}' | %(prog)s

'''

'''force return from the server in JSON format'''
HEADERS = {'content-type': 'application/json'}


def main():

    import argparse
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--infile', '-i',
                        help="File containing the JSON object as a JSON string.")
    parser.add_argument('--server',
                        help="Full URL of the server.")
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile\
                        =%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--authid',
                        help="The HTTP auth ID.")
    parser.add_argument('--authpw',
                        help="The HTTP auth PW.")
    parser.add_argument('--force-put',
                        default=False,
                        action='store_true',
                        help="Force the object to be PUT rather than PATCHed.  \
                        Default is False.")
    parser.add_argument('--get-only',
                        default=False,
                        action='store_true',
                        help="Do nothing but get the object and print it.  \
                        Default is False.")
    parser.add_argument('--id',
                        help="URI for an object"),
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    args = parser.parse_args()

    global DEBUG_ON
    DEBUG_ON = args.debug

    if args.get_only:
        GET_ONLY = True
    else:
        GET_ONLY = False

    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    new_object = False
    if args.id:
        GET_ONLY = True
        print("Taking id to get from --id")
        new_json = {}
        uuid_response = {}
        accession_response = {}
        try:
            id_response = encodedcc.get_ENCODE(args.id, connection)
        except:
            id_response = {}
            new_object = True
    else:
        if args.infile:
            infile = open(args.infile, 'r')
        else:
            infile = sys.stdin

        new_json_string = infile.read()

        new_json = json.loads(new_json_string)
        if '@id' in new_json:
            try:
                id_response = encodedcc.get_ENCODE(new_json['@id'], connection)
            except:
                id_response = {}
                new_object = True
        else:
            id_response = {}
        if 'uuid' in new_json:
            try:
                uuid_response = encodedcc.get_ENCODE(new_json['uuid'], connection)
            except:
                uuid_response = {}
                new_object = True
        else:
            uuid_response = {}
        if 'accession' in new_json:
            try:
                accession_response = encodedcc.get_ENCODE(new_json['accession'], connection)
            except:
                accession_response = {}
                new_object = True
        else:
            print("No identifier in new JSON object.  Assuming POST or PUT with auto-accessioning.")
            new_object = True
            accession_response = {}

    object_exists = False
    if id_response:
        object_exists = True
        print("Found matching @id:")
        encodedcc.pprint_ENCODE(id_response)
    if uuid_response:
        object_exists = True
        print("Found matching uuid:")
        encodedcc.pprint_ENCODE(uuid_response)
    if accession_response:
        object_exists = True
        print("Found matching accession")
        encodedcc.pprint_ENCODE(accession_response)

    if id_response and uuid_response and (id_response != uuid_response):
        print("Existing id/uuid mismatch")
    if id_response and accession_response and (id_response != accession_response):
        print("Existing id/accession mismatch")
    if uuid_response and accession_response and (uuid_response != accession_response):
        print("Existing uuid/accession mismatch")

    if new_object and object_exists:
        print("Conflict:  At least one identifier already exists and at least one does not exist")

    supported_collections = ['access_key', 'antibody_approval',
                             'antibody_characterization', 'antibody_lot',
                             'award', 'biosample', 'biosample_characterization',
                             'construct', 'construct_characterization', 'dataset',
                             'document', 'donor', 'edw_key', 'experiment', 'file',
                             'file_relationship', 'human_donor', 'lab', 'library',
                             'mouse_donor', 'organism', 'platform', 'replicate',
                             'rnai', 'rnai_characterization', 'software',
                             'source', 'target', 'treatment', 'user',
                             'analysis_step_run', 'pipeline', 'workflow_run',
                             'analysis_step', 'software_version', 'publication']

    def convert(name):
        '''used to convert CamelCase text to snake_case'''
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    type_list = new_json.pop('@type', [])
    possible_collections = [convert(x) for x in type_list if x in supported_collections]
    if possible_collections:
        # collection = possible_collections[0] + 's/'
        collection = possible_collections[0]
    else:
        collection = []
    if '@id' in new_json:
        identifier = new_json.pop('@id')
    elif 'uuid' in new_json:
        if collection:
            identifier = '/' + collection + '/' + new_json['uuid'] + '/'
        else:
            identifier = '/' + new_json['uuid'] + '/'
    elif 'accession' in new_json:
        if collection:
            identifier = '/' + collection + '/' + new_json['accession'] + '/'
        else:
            identifier = '/' + new_json['accession'] + '/'
    if 'attachment' in new_json:
        if 'href' in new_json['attachment']:
            pass
        else:
            try:
                filename = new_json['attachment']['download']
                print("Setting filename to %s" % (filename))
            except:
                print("Must specify either href or filename for attachment", file=sys.stderr)
            if new_json['attachment'].get('type'):
                mime_type = new_json['attachment'].get('type')
            else:
                try:
                    mime_type, encoding = mimetypes.guess_type(filename)
                    major, minor = mime_type.split('/')
                    #detected_type = magic.from_file(filename, mime=True)
                    print("Detected mime type %s" % (mime_type))
                except:
                    print("Failed to detect mime type in file %s" % (filename), file=sys.stderr)
            try:
                with open(filename, 'rb') as stream:
                    print("opened")
                    newvalue = {
                        'download': filename,  # Just echoes the given filename as the download name
                        'type': mime_type,
                        'href': 'data:%s;base64,%s' % (mime_type, b64encode(stream.read()))
                    }
                f = open('tmp', 'w')
                print(f, newvalue)
                new_json.update({'attachment': newvalue})  # add
            except:
                print("Cannot open file %s" % (filename), file=sys.stderr)
    if object_exists:
        if args.force_put:
            if not GET_ONLY:
                print("Replacing existing object")
                encodedcc.replace_ENCODE(identifier, connection, new_json)
        else:
            if not GET_ONLY:
                print("Patching existing object")
                encodedcc.patch_ENCODE(identifier, connection, new_json)
    elif new_object:
        if args.force_put:
            if not GET_ONLY:
                print("PUT'ing new object")
                encodedcc.replace_ENCODE(identifier, connection, new_json)
        else:
            if not GET_ONLY:
                print("POST'ing new object")
                encodedcc.new_ENCODE(collection, connection, new_json)


if __name__ == '__main__':
    main()
