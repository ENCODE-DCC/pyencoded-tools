#!/usr/bin/env python3
# -*- coding: latin-1 -*-
'''Take a comma-delimited, double quote-quoted csv and update ENCODE objects appropriately'''

import requests
import json
import os
import csv
import subprocess
import time
import logging
import common
from encodedcc import ENC_Key, ENC_Connection, ENC_Item
# from StringIO import StringIO

logger = logging.getLogger(__name__)

EPILOG = '''Notes:
    Requires comma-delimited, double quote-quoted (for values containing commas or newlines)
    csv's should have a header as below and data rows in the form:

    accession,property1:array,property2,property3:int,property4:float ...
    ENCBSxyzabc,"[""elementwithcomma,andstuff"",""second element""]",stringproperty,3,4.1 ...
    ...
    Note that array elements are comma-delimited within a double-quote quoted string
    Excel will export to csv in exactly that format automatically.  Following the example, the
    cell should contain exactly the string ["elementwithcomma,andstuff","second element"]
    Only arrays of strings are supported
    Int and float are supported as property:int and property:float

    To update an existing object, the accession (or uuid or other valid identifier) must be given,
    must exist, and the given properties must be in its schema.
    If the property has a value and does not exist in the object it is added with the specified value.
    If the property has a value and exists in the object, its value is over-written.
    If the property exists, there is no new value, and --put has been given, the property will be removed altogether.

    To create new objects, omit uuid and accession, but add @type, which must be the object schema
    name (like human_donor, not human-donors).

    New file object creation and uploading are supported.
    Give the path and file name in the submmitted_file_name property.  Only the base filename will be posted to submitted_file_name
    md5sum and file_size will be calculated.
    Upload to AWS requires your AWS credentials to be set up.

    Each object's identifier is echo'ed to stdout as the script works on it.

Examples:

    %(prog)s --key www --infile note2notes.csv

'''


def upload_file(credentials, f_path):
    # upload to S3
    env = os.environ.copy()
    env.update({
        'AWS_ACCESS_KEY_ID': credentials['access_key'],
        'AWS_SECRET_ACCESS_KEY': credentials['secret_key'],
        'AWS_SECURITY_TOKEN': credentials['session_token'],
    })

    logger.info("Uploading file.")
    start = time.time()
    try:
        subprocess.check_call(
            ['aws', 's3', 'cp', f_path, credentials['upload_url'], '--quiet'], env=env)
    except subprocess.CalledProcessError as e:
        # The aws command returns a non-zero exit code on error.
        logger.error("Upload failed with exit code %d" % e.returncode)
        upload_returncode = e.returncode
    else:
        upload_returncode = 0
        end = time.time()
        duration = end - start
        logger.info("Uploaded in %.2f seconds" % duration)

    return upload_returncode


def get_upload_credentials(server, keypair, accession):
    r = requests.get()


def main():

    import argparse
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--key', default='default',
                        help="The keypair identifier from the keyfile for the server.  Default is --key=default")

    parser.add_argument('--keyfile', default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))

    parser.add_argument('--infile', '-i',
                        help="CSV file with metadata to update")

    parser.add_argument('--dryrun', default=False, action='store_true',
                        help="Do everything except save changes")

    parser.add_argument('--debug', default=False, action='store_true',
                        help="Print debug messages.  Default is False.")

    parser.add_argument('--put', default=False, action='store_true',
                        help="If property in the input is blank, remove that property entirely from the existing object")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(
            format='%(levelname)s:%(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(
            format='%(levelname)s:%(message)s', level=logging.INFO)

    key = ENC_Key(args.keyfile, args.key)  # get the keypair
    connection = ENC_Connection(key)  # initialize the connection object
    # biosample_collection = ENC_Collection(connection,'biosamples',frame='object')

    with open(args.infile, 'rU') as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"')
        for new_metadata in reader:
            uuid = new_metadata.pop('uuid', None)
            accession = new_metadata.pop('accession', None)
            if uuid:  # use the uuid if there is one
                obj_id = uuid
            elif accession:  # if no uuid then use the accession
                obj_id = accession
            else:  # if neither uuid or accession, assume this is a new object
                obj_id = None
            enc_object = ENC_Item(connection, obj_id)
            # print "Got accessioned object %s with status %s" %(enc_object.get('accession'), enc_object.get('status'))
            for prop in new_metadata:
                if new_metadata[prop].strip() == "":
                    if args.put:  # if empty, pop out the old property from the object
                        old_value = enc_object.properties.pop(prop, None)
                    continue  # skip properties with no value for post or patch
                else:  # new property or new value for old property
                    new_metadata_string = new_metadata[prop]
                    if ':' in prop:
                        prop_name, sep, prop_type = prop.partition(':')
                    else:
                        prop_name = prop
                        prop_type = 'string'
                    if prop_type == 'array':
                        # subreader = csv.reader(StringIO(new_metadata_string), delimiter=',', quotechar='"')
                        # array_items = []
                        # for line in subreader:
                        #   for s in line:
                        #       array_items.append(s)
                        print("new_metadata_string is %s" %
                              (new_metadata_string))
                        array_items = json.loads(new_metadata_string)
                        print("array_items is %s" % (array_items))
                        json_obj = {prop_name: array_items}
                    elif prop_type == 'int' or prop_type == 'integer':
                        json_obj = {prop_name: int(new_metadata_string)}
                    elif prop_type == 'float':
                        json_obj = {prop_name: float(new_metadata_string)}
                    else:
                        # default is string
                        json_obj = {prop_name: new_metadata_string}
                    enc_object.properties.update(json_obj)
            if 'submitted_file_name' in enc_object.properties:
                path = os.path.expanduser(
                    enc_object.get('submitted_file_name'))
                path = os.path.abspath(path)
                basename = os.path.basename(path)
                enc_object.properties.update({
                    'submitted_file_name': basename,
                    'md5sum': common.md5(path),
                    'file_size': os.path.getsize(path)})
            if obj_id:
                logger.info('Syncing %s' % (obj_id))
            else:
                logger.info('Syncing new object')
            logger.debug('%s' % (json.dumps(enc_object.properties,
                                            sort_keys=True, indent=4, separators=(',', ': '))))
            if not args.dryrun:
                new_object = enc_object.sync()
                try:
                    new_accession = new_object['accession']
                except:
                    pass
                else:
                    print("New accession: %s" % (new_accession))
                    if enc_object.type == 'file' and 'submitted_file_name' in json_obj:
                        upload_credentials = enc_object.new_creds()
                        print(upload_credentials)
                        rc = upload_file(upload_credentials, path)
                        print("Upload rc: %d" % (rc))


if __name__ == '__main__':
    main()
