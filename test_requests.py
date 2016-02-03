import argparse
import os.path
import sys
import encodedcc
import requests
import logging
from urllib.parse import urljoin
from urllib.parse import quote
from lxml import html
import json

# http://stackoverflow.com/questions/27652543/how-to-use-python-requests-to-fake-a-browser-visit

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--infile',
                        help="single column list of object accessions")
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
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH the data.  Default is False")
    args = parser.parse_args()
    return args


def get_ENCODE(obj_id, connection, frame="object"):
    '''GET an ENCODE object as JSON and return as dict'''
    if frame is None:
        if '?' in obj_id:
            url = urljoin(connection.server, obj_id+'&limit=all')
        else:
            url = urljoin(connection.server, obj_id+'?limit=all')
    elif '?' in obj_id:
        url = urljoin(connection.server, obj_id+'&limit=all&frame='+frame)
    else:
        url = urljoin(connection.server, obj_id+'?limit=all&frame='+frame)
    logging.debug('GET %s' % (url))
    response = requests.get(url, auth=connection.auth, headers=connection.headers)
    logging.debug('GET RESPONSE code %s' % (response.status_code))
    try:
        if response.json():
            logging.debug('GET RESPONSE JSON: %s' % (json.dumps(response.json(), indent=4, separators=(',', ': '))))
    except:
        logging.debug('GET RESPONSE text %s' % (response.text))
    if not response.status_code == 200:
        if response.json().get("notification"):
            logging.warning('%s' % (response.json().get("notification")))
        else:
            logging.warning('GET failure.  Response code = %s' % (response.text))
    return response.json()


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    url = "https://www.encodeproject.org/search/?type=Experiment"
    headers = {'content-type': 'application/json', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    response = requests.get(url, headers=headers)
    tree = html.fromstring(response.content)
    facets = tree.xpath('//div[@class="box facets vertical"]/text()')
    # <div class="box facets vertical" data-reactid=".1cqd3e45q80.1.1.0.1.$/search/?type=0Experiment.0.0.0.0.0.0">
    print(facets)

if __name__ == '__main__':
        main()
