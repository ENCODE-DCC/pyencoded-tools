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
python3 ENCODE_replaced_cleaner.py --keyfile keypairs.json --key test
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
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


def retreive_list_of_replaced(object_type,
                              object_to_inspect_acc,
                              keypair, server):
    url = server + 'search/?type=Item&accession=' + \
        object_to_inspect_acc + '&format=json&limit=all'
    to_return_list = [object_to_inspect_acc]
    objects_to_inspect = encoded_get(url, keypair)['@graph']
    if objects_to_inspect:
        for object_to_inspect in objects_to_inspect:
            if object_to_inspect.get('alternate_accessions'):
                for acc in object_to_inspect.get('alternate_accessions'):
                    to_return_list.extend(
                        retreive_list_of_replaced(
                            object_type, acc, keypair, server))
                return to_return_list
            else:
                return to_return_list
    else:
        return to_return_list


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    keypair = (key.authid, key.authpw)
    server = key.server
    connection = encodedcc.ENC_Connection(key)

    url = server + 'profiles/?format=json&limit=all'
    profiles = encoded_get(url, keypair)

    for object_type in profiles.keys():
        changes = {}
        sets = []
        url = server + 'search/?type=' + object_type + '&format=json&limit=all'
        objects = encoded_get(url, keypair)['@graph']
        for entry in objects:
            if entry.get('alternate_accessions'):
                replaced_objects_accs = []
                for acc in entry.get('alternate_accessions'):
                    replaced_objects_accs.extend(
                        retreive_list_of_replaced(object_type, acc))
                if sorted(list(set(replaced_objects_accs))) != sorted(
                   entry.get('alternate_accessions')):
                    changes[entry['uuid']] = set(replaced_objects_accs)
                    sets.append(set(replaced_objects_accs))

        needs_update = 0
        for k in changes.keys():
            k_counter = 0
            for s in sets:
                if changes[k] <= s:
                    k_counter += 1
            if k_counter == 1:
                needs_update += 1
                for acc in list(changes[k]):
                    ob_url = server + 'search/?type=Item&accession=' + acc
                    obs = encoded_get(ob_url, keypair)['@graph']
                    for ob in obs:
                        print (ob['uuid'] + ' alternate accessions list ' +
                               str(ob['alternate_accessions']) + ' is removed')
                        encodedcc.patch_ENCODE(
                            ob['uuid'],
                            connection,
                            {"alternate_accessions": []})

                print (k + ' is patched with ' + str({"alternate_accessions":
                                                      list(changes[k])}))
                encodedcc.patch_ENCODE(
                    k,
                    connection,
                    {"alternate_accessions": list(changes[k])})

if __name__ == '__main__':
    main()
