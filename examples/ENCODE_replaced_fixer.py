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
        if (not entry.startswith('/files/ENCFF')) and \
           (not entry.startswith('/files/TSTFF')):

            replaced_file = encoded_get(server +
                                        entry,
                                        keypair)
            if replaced_file['uuid'] == entry.split('/')[2]:
                check_for_existance = encoded_get(
                    server + replaced_file['accession'], keypair)
                if check_for_existance.get('status') != 'error':
                    new_entry = '/files/' + \
                                check_for_existance['accession'] + '/'
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


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    keypair = (key.authid, key.authpw)
    server = key.server
    query = args.query

    files = encoded_get(server +
                        'search/?type=File&format=json&' +
                        'frame=object&limit=all&' + query,
                        keypair)['@graph']
    print ('There are ' + str(len(files)) + ' files on the portal')
    counter = 0
    for f in files:
        counter += 1
        if counter % 1000 == 0:
            print ('Script processed ' + str(counter) + ' files')
        if f['status'] not in ['replaced']:
            patching_data = {}
            derived_from_list = f.get('derived_from')
            if derived_from_list:
                new_derived_from_list = process_links_list(
                    derived_from_list, keypair, server)
                if new_derived_from_list:
                    patching_data['derived_from'] = new_derived_from_list

            paired_with_file = f.get('paired_with')
            if paired_with_file:
                new_paired_with = process_links_list(
                    [paired_with_file], keypair, server)
                if new_paired_with:
                    patching_data['paired_with'] = new_paired_with[0]

            controlled_by_list = f.get('controlled_by')
            if controlled_by_list:
                new_controlled_by_list = process_links_list(
                    controlled_by_list, keypair, server)
                if new_controlled_by_list:
                    patching_data['controlled_by'] = new_controlled_by_list
            if patching_data:
                print ('Patching file ' + f['accession'])
                encodedcc.patch_ENCODE(f['accession'],
                                       connection, patching_data)
                



if __name__ == '__main__':
    main()