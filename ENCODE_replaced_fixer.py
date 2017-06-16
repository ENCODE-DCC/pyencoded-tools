import argparse
import os.path
import encodedcc
import requests
import urllib.parse
from time import sleep

import sys
GET_HEADERS = {'accept': 'application/json'}


EPILOG = '''
For more details:

        %(prog)s --help
'''
'''
Example command:
python3 ENCODE_replaced_fixer.py --keyfile keypairs.json --key test  --query 'accession=ENCFF123ABC'
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--query', default='',
                        help="override the file search query, e.g. 'accession=ENCFF000ABC'")
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
    args = parser.parse_args()
    return args


def encoded_get(url, keypair=None, frame='object', return_response=False):
    url_obj = urllib.parse.urlsplit(url)
    new_url_list = list(url_obj)
    query = urllib.parse.parse_qs(url_obj.query)
    if 'format' not in query:
        new_url_list[3] += "&format=json"
    if 'frame' not in query:
        new_url_list[3] += "&frame=%s" % (frame)
    if 'limit' not in query:
        new_url_list[3] += "&limit=all"
    if new_url_list[3].startswith('&'):
        new_url_list[3] = new_url_list[3].replace('&', '', 1)
    get_url = urllib.parse.urlunsplit(new_url_list)
    max_retries = 10
    max_sleep = 10
    while max_retries:
        try:
            if keypair:
                response = requests.get(get_url,
                                        auth=keypair,
                                        headers=GET_HEADERS)
            else:
                response = requests.get(get_url, headers=GET_HEADERS)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError) as e:
            print >> sys.stderr, e
            sleep(max_sleep - max_retries)
            max_retries -= 1
            continue
        else:
            if return_response:
                return response
            else:
                return response.json()


def process_links_list(list_of_links, keypair, server):
    to_return_list = set()
    for entry in list_of_links:
        if (entry.find('ENC') == -1) and \
           (entry.find('TST') == -1):
            replaced_object = encoded_get(server +
                                          entry,
                                          keypair)
            acc = replaced_object.get('accession')
            if acc:
                check_for_existance = encoded_get(
                    server + acc, keypair)
                if check_for_existance.get('status') != 'error':
                    new_entry = check_for_existance.get('@id')
                else:
                    new_entry = entry
            else:
                new_entry = entry
            to_return_list.add(new_entry)
        else:
            to_return_list.add(entry)
    if sorted(list_of_links) != sorted(list(to_return_list)):
        return list(to_return_list)
    return None


def fix_replaced_references(obj, property, patching_data, keypair, server):
    obj_property = obj.get(property)
    if obj_property:
        if isinstance(obj_property, list):
            new_links_list = process_links_list(
                obj_property, keypair, server)
            if new_links_list:
                patching_data[property] = new_links_list
        else:
            new_links_list = process_links_list(
                [obj_property], keypair, server)
            if new_links_list:
                patching_data[property] = new_links_list[0]


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    # connection = encodedcc.ENC_Connection(key)
    keypair = (key.authid, key.authpw)
    server = key.server
    query = args.query

    objects = \
        encoded_get(server + 'search/?type=AntibodyLot' +
                    '&type=Donor&type=Biosample' +
                    '&type=File&type=Library' +
                    '&type=Dataset&type=Pipeline' +
                    '&type=Replicate' +
                    '&type=Treatment&format=json&' +
                    'frame=object&limit=5000&' + query, keypair)['@graph']
    print ('There are ' + str(len(objects)) +
           ' objects that should be inspected on the portal')
    counter = 0
    for obj in objects:
        counter += 1
        if counter % 1000 == 0:
            print ('Script processed ' + str(counter) + ' objects')
        if obj['status'] not in ['replaced']:
            patching_data = {}

            # fixing links of donor
            fix_replaced_references(obj, 'parent_strains',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'identical_twin',
                                    patching_data, keypair, server)

            # fixing links of file/experiment/biosample
            fix_replaced_references(obj, 'derived_from',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'paired_with',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'controlled_by',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'supersedes',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'dataset',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'related_files',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'related_datasets',
                                    patching_data, keypair, server)

            # fixing links of biosample
            fix_replaced_references(obj, 'host',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'part_of',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'originated_from',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'pooled_from',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'donor',
                                    patching_data, keypair, server)

            # fixing links of library
            fix_replaced_references(obj, 'biosample',
                                    patching_data, keypair, server)

            # fixing links of treatment
            fix_replaced_references(obj, 'biosamples_used',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'antibodies_used',
                                    patching_data, keypair, server)

            # fixing links of replicate
            fix_replaced_references(obj, 'antibody',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'experiment',
                                    patching_data, keypair, server)
            fix_replaced_references(obj, 'library',
                                    patching_data, keypair, server)
            if patching_data:
                print ('Patching object ' + obj['@type'][0] + '\t' + obj['uuid'] + '\t' + str(patching_data))
                # encodedcc.patch_ENCODE(obj['uuid'],
                #                       connection, patching_data)


if __name__ == '__main__':
    main()
