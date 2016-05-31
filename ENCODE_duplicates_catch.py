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
python3 ENCODE_duplicates_catch.py --keyfile keypairs.json --key test 
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


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    keypair = (key.authid, key.authpw)
    server = key.server

    files = encoded_get(server +
                        'search/?type=File&format=json&' +
                        'frame=object&limit=all',
                        keypair)['@graph']
    interesting_files = [f for f in files if ((f['status'] not in ['replaced'])
                         and ('content_md5sum' in f))]

    content_dictionary = {}
    mone = 0
    for interesting_file in interesting_files:
        file_con = interesting_file['content_md5sum']
        if file_con not in content_dictionary:
            content_dictionary[file_con] = []
        content_dictionary[file_con].append(interesting_file)
        mone += 1
        if mone % 1000 == 0:
            print ("screened through " + str(mone) + " files")

    duplicate_counter = 0
    lab_dictionary = {}

    for key in content_dictionary.keys():
        if len(content_dictionary[key]) > 1:
            duplicate_counter += 1
            lab_id = ''
            for x in content_dictionary[key]:
                lab_id = x['lab']
            if lab_id not in lab_dictionary:
                lab_dictionary[lab_id] = []
            lab_dictionary[lab_id].append(content_dictionary[key])

    for k in lab_dictionary.keys():
        print ('LAB with DUPLICATES : ' + k)
        print ('NUM of replicate cases : ' + str(len(lab_dictionary[k])))
        for entry in lab_dictionary[k]:
            for x in entry:
                print (x['accession'] + '\t' + x['dataset'] +
                       '\t'+x['content_md5sum']+'\t'+x['file_format'] +
                       '\t'+x['status'])


if __name__ == '__main__':
    main()
