#!/usr/bin/env python3
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
    parser.add_argument('--frame',
                        help="define a frame to get back the JSON object, for use with --id. Default is frame=object",
                        default="object")
    parser.add_argument('--type',
                        help="the object's type")
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH/POST the data.  Default is False")
    args = parser.parse_args()

    global DEBUG_ON
    DEBUG_ON = args.debug

    if args.get_only:
        GET_ONLY = True
    else:
        GET_ONLY = False

    key = encodedcc.ENC_Key(args.keyfile, args.key)
    if args.server and args.authpw and args.authid:
        key.server = args.server
        key.authid = args.authid
        key.authpw = args.authpw
        print("Creating authorization data from command line inputs")
    connection = encodedcc.ENC_Connection(key)
    print("Running on {}".format(connection.server))
    if args.update:
        print("This is an UPDATE run! Data will be PATCHed or POSTed accordingly")
    else:
        print("This is a dry run, no data will be changed")

    new_object = False
    if args.id:
        GET_ONLY = True
        print("Taking id to get from --id")
        new_json = {}
        uuid_response = {}
        accession_response = {}
        try:
            id_response = encodedcc.get_ENCODE(
                args.id, connection, frame=args.frame)
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
        if args.debug:
            encodedcc.pprint_ENCODE(new_json)
        if '@id' in new_json:
            id_response = encodedcc.get_ENCODE(new_json['@id'], connection)
            if id_response.get("code") == 404:
                id_response = {}
                new_object = True
        else:
            id_response = {}
            new_object = True
        if 'uuid' in new_json:
            uuid_response = encodedcc.get_ENCODE(new_json['uuid'], connection)
            if uuid_response.get("code") == 404:
                uuid_response = {}
                new_object = True
        else:
            uuid_response = {}
            new_object = True
        if 'accession' in new_json:
            accession_response = encodedcc.get_ENCODE(
                new_json['accession'], connection)
            if accession_response.get("code") == 404:
                accession_response = {}
                new_object = True
        else:
            accession_response = {}
            new_object = True

        if new_object:
            print(
                "No identifier in new JSON object.  Assuming POST or PUT with auto-accessioning.")

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

    profiles = encodedcc.get_ENCODE("/profiles/", connection)
    supported_collections = list(profiles.keys())
    if "Dataset" not in supported_collections:
        supported_collections.append("Dataset")

    type_list = new_json.pop('@type', [])
    if args.type:
        type_list = [args.type]
    if any(type_list):
        findit = False
        for x in supported_collections:
            if x.lower() == type_list[0].lower():
                type_list = [x]
                findit = True
        if findit:
            if args.debug:
                print("Object will have type of", type_list[0])
        else:
            print("Error! JSON object does not contain one of the supported types")
            print("Provided type:", type_list[0])
            print(
                "Please either change the JSON file or define the type with the --type feature")
            sys.exit(1)
    else:
        print("No type found for JSON object!")
        sys.exit(1)

    possible_collections = [x for x in type_list if x in supported_collections]
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
                print(
                    "Must specify either href or filename for attachment", file=sys.stderr)
            if new_json['attachment'].get('type'):
                mime_type = new_json['attachment'].get('type')
            else:
                try:
                    mime_type, encoding = mimetypes.guess_type(filename)
                    major, minor = mime_type.split('/')
                    #detected_type = magic.from_file(filename, mime=True)
                    print("Detected mime type %s" % (mime_type))
                except:
                    print("Failed to detect mime type in file %s" %
                          (filename), file=sys.stderr)
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
                if args.update:
                    e = encodedcc.replace_ENCODE(
                        identifier, connection, new_json)
                    print(e)
        else:
            if not GET_ONLY:
                print("PATCHing existing object")
                if args.update:
                    e = encodedcc.patch_ENCODE(
                        identifier, connection, new_json)
                    print(e)
    elif new_object:
        if args.force_put:
            if not GET_ONLY:
                print("PUT'ing new object")
                if args.update:
                    e = encodedcc.replace_ENCODE(
                        identifier, connection, new_json)
                    print(e)
        else:
            if not GET_ONLY:
                print("POST'ing new object")
                if not any(collection):
                    print(
                        "ERROR: Unable to POST to non-existing collection {}".format(collection))
                    sys.exit(1)
                if args.update:
                    e = encodedcc.new_ENCODE(connection, collection, new_json)
                    print(e)


if __name__ == '__main__':
    main()
