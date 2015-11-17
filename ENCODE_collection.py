#!/usr/bin/env python
# -*- coding: latin-1 -*-
'''GET an ENCODE collection and output a tsv of all objects and
their properties suitable for spreadsheet import'''
import json
import os.path
import encodedcc
import requests
from urllib.parse import urljoin
import logging

EPILOG = '''Limitations:

Gets all objects, no searching implemented yet.
Arrays of strings come back with the items quoted, SO NOT SUITABLE FOR DIRECT PATCH back

Examples:

Dump all the biosamples, with all schema properties, from the default server with default keypair and save to a file

    # be patient, this takes some time to run
    %(prog)s biosamples > biosamples.tsv

Same for human-donors

    %(prog)s human-donors > human-donors.tsv

'''


def get_without_ESearch(obj_id, connection):
    '''GET an ENCODE object as JSON and return as dict'''
    if '?' in obj_id:
        url = urljoin(connection.server, obj_id)
    else:
        url = urljoin(connection.server, obj_id)
    logging.debug('GET %s' % (url))
    response = requests.get(url, auth=connection.auth, headers=connection.headers)
    logging.debug('GET RESPONSE code %s' % (response.status_code))
    try:
        if response.json():
            logging.debug('GET RESPONSE JSON: %s' % (json.dumps(response.json(), indent=4, separators=(',', ': '))))
    except:
        logging.debug('GET RESPONSE text %s' % (response.text))
    if not response.status_code == 200:
        logging.warning('GET failure.  Response code = %s' % (response.text))
    return response.json()


def main():

    import argparse
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('collection',
                        help="The collection to get")
    parser.add_argument('--es',
                        default=False,
                        action='store_true',
                        help="Use elasticsearch")
    parser.add_argument('--query',
                        help="A complete query to run rather than GET the whole collection.  \
                        E.g. \"search/?type=biosample&lab.title=Ross Hardison, PennState\".  Implies --es.")
    parser.add_argument('--submittable',
                        default=False,
                        action='store_true',
                        help="Show only properties you might want a submitter to submit.")
    parser.add_argument('--server',
                        help="Full URL of the server.")
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --\
                        keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--authid',
                        help="The HTTP auth ID.")
    parser.add_argument('--authpw',
                        help="The HTTP auth PW.")
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")

    args = parser.parse_args()

    keys = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(keys)

    global DEBUG
    DEBUG = args.debug

    supplied_name = args.collection

    if supplied_name.endswith('s'):
        schema_name = supplied_name.rstrip('s').replace('-', '_') + '.json'
    elif supplied_name.endswith('.json'):
        schema_name = supplied_name
    else:
        schema_name = supplied_name.replace('-', '_') + '.json'

    schema_uri = '/profiles/' + schema_name
    object_schema = encodedcc.get_ENCODE(schema_uri, connection)
    headings = []
    for schema_property in object_schema["properties"]:
        property_type = object_schema["properties"][schema_property]["type"]
        if isinstance(property_type, list):  # hack to deal with multi-typed properties, just pick the first one
            property_type = property_type[0]
        if property_type == 'string':  # if it's a string type, the heading is just the property name
            headings.append(schema_property)
        elif property_type == 'array':  # format the heading to be property_name:type:array or, if an array of strings, property_name:array
            if 'items' in object_schema["properties"][schema_property].keys():
                whateveritscalled = "items"
            elif 'reference' in object_schema["properties"][schema_property].keys():
                whateveritscalled = "reference"
            elif 'url' in object_schema["properties"][schema_property].keys():
                whateveritscalled = "url"
            else:
                print(object_schema["properties"][schema_property].keys())
                raise NameError("None of these match anything I know")
            if object_schema["properties"][schema_property][whateveritscalled]["type"] == 'string':
                headings.append(schema_property + ':array')
            else:
                try:
                    headings.append(schema_property + ':' + object_schema["properties"][schema_property][whateveritscalled]["type"] + ':array')
                except:
                    headings.append(schema_property + ':mixed:array')
        else:  # it isn't a string, and it isn't an array, so make the heading property_name:type
            headings.append(schema_property + ':' + property_type)
    headings.sort()
    if 'file' in supplied_name or 'dataset' in supplied_name or 'source' in supplied_name or 'award' in supplied_name:
        pass
    else:
        # headings.append('award.rfa') #need to add a parameter to specify additional properties
        pass
    if 'file' in supplied_name:
        headings.append('replicate.biological_replicate_number')
        headings.append('replicate.technical_replicate_number')
    if 'biosample' in supplied_name:
        headings.append('organ_slims')
    if 'access-key' in supplied_name:
        headings.append('user.title')
    if 'user' in supplied_name:
        headings.append('title')

    exclude_unsubmittable = ['accession', 'uuid', 'schema_version', 'alternate_accessions', 'submitted_by']

    global collection
    if args.query:
        uri = args.query
        collection = encodedcc.get_ENCODE(uri, connection)
    elif args.es:
        uri = '/search/?format=json&limit=all&type=' + supplied_name
        collection = encodedcc.get_ENCODE(uri, connection)
    else:
        collection = get_without_ESearch(supplied_name, connection)
    collected_items = collection['@graph']

    headstring = ""
    for heading in headings:
        if args.submittable and heading.split(':')[0] in exclude_unsubmittable:
            pass
        else:
            headstring += heading + '\t'
    headstring = headstring.rstrip()
    print(headstring)

    for item in collected_items:
        # obj = encodedcc.get_ENCODE(item['@id'], connection)
        obj = item
        obj = encodedcc.flat_ENCODE(obj)
        rowstring = ""
        for header in headstring.split('\t'):
            prop_key = header.split(':')[0]
            if prop_key in obj:
                tempstring = json.dumps(obj[prop_key]).lstrip('"').rstrip('"')
                if tempstring == '[]':
                    tempstring = ""
                rowstring += tempstring + '\t'
            elif '.' in prop_key:
                try:
                    embedded_key = obj[prop_key.split('.')[0]]
                    if '/' in embedded_key:
                        embedded_obj = encodedcc.get_ENCODE(embedded_key, connection)
                    else:
                        embedded_obj = encodedcc.get_ENCODE(prop_key.split('.')[0] + '/' + obj[prop_key.split('.')[0]], connection)
                    embedded_value_string = json.dumps(embedded_obj[prop_key.split('.')[1]]).lstrip('"').rstrip('"')
                    if embedded_value_string == '[]':
                        embedded_value_string = ""
                except KeyError:
                    embedded_value_string = ""
                rowstring += embedded_value_string + '\t'
            else:
                rowstring += '\t'
        rowstring = rowstring.rstrip()
        print(rowstring)

if __name__ == '__main__':
    main()
