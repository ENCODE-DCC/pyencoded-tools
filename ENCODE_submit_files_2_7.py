import hashlib
import os.path
import subprocess
import time
import csv
import logging
import json
import sys
import requests
import gzip
from io import BytesIO
from urlparse import urljoin
from urllib import quote

logger = logging.getLogger(__name__)
logging.basicConfig(filename="log.txt", filemode="w", format='%(message)s')
#############################
# Set defaults


class ENC_Key:
    def __init__(self, keyfile, keyname):
        keys_f = open(keyfile, 'r')
        keys_json_string = keys_f.read()
        keys_f.close()
        keys = json.loads(keys_json_string)
        key_dict = keys[keyname]
        self.authid = key_dict['key']
        self.authpw = key_dict['secret']
        self.server = key_dict['server']
        if not self.server.endswith("/"):
            self.server += "/"


class ENC_Connection(object):
    def __init__(self, key):
        self.headers = {'content-type': 'application/json'}
        self.server = key.server
        self.auth = (key.authid, key.authpw)


class NewFile():
    def __init__(self, dictionary, connection):
        self.post_input = {}
        self.connection = connection
        # get controlled_by list
        if dictionary.get("controlled_by"):
            control = dictionary.pop("controlled_by")
            self.post_input["controlled_by"] = control.split(",")

        # get aliases list
        if dictionary.get("aliases"):
            alias = dictionary.pop("aliases")
            self.post_input["aliases"] = alias.split(",")

        # make flowcell dict
        flowcell_dict = {}
        for val in ["lane", "barcode", "flowcell", "machine"]:
            flowcell_dict[val] = dictionary.pop(val)
        # add flowcell_details to post_input
        self.post_input["flowcell_details"] = [flowcell_dict]

        # calculate md5sum
        md5sum = hashlib.md5()
        path = dictionary.pop("file_path")
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024*1024), b''):
                md5sum.update(chunk)
        # add md5sum to post_input
        self.post_input["md5sum"] = md5sum.hexdigest()

        # fill in rest of post_input
        for key in dictionary.keys():
            if key == "submitted_file_name":
                if any(dictionary.get("submitted_file_name")):
                    self.post_input["submitted_file_name"] = dictionary["submitted_file_name"]
                else:
                    self.post_input["submitted_file_name"] = path.rsplit("/", 1)[-1]
            else:
                if dictionary.get(key):
                    self.post_input[key] = dictionary[key]

        # if fastq get the read_length
        if dictionary.get("file_format") == "fastq":
            for header, sequence, qual_header, quality in self.fastq_read(self.connection, filename=path):
                sequence = sequence.decode("UTF-8")
                read_length = len(sequence)
            self.post_input["read_length"] = read_length

    def post_file(self):
        ####################
        # POST metadata
        r = self.new_ENCODE(self.connection, "files", self.post_input)
        print repr(r)
        #####################
        # POST file to S3
        item = r["@graph"][0]
        creds = item['upload_credentials']
        env = os.environ.copy()
        env.update({
            'AWS_ACCESS_KEY_ID': creds['access_key'],
            'AWS_SECRET_ACCESS_KEY': creds['secret_key'],
            'AWS_SECURITY_TOKEN': creds['session_token'],
        })
        print "Uploading file"
        path = self.data["file_path"]
        print path
        start = time.time()
        subprocess.check_call(['aws', 's3', 'cp', path, creds['upload_url']], env=env)
        end = time.time()
        duration = end - start
        print "Uploaded in %.2f seconds" % duration

    def new_ENCODE(self, connection, collection_name, post_input):
        '''POST an ENCODE object as JSON and return the response JSON
        '''
        if isinstance(post_input, dict):
            json_payload = json.dumps(post_input)
        elif isinstance(post_input, str):
            json_payload = post_input
        else:
            print 'Datatype to POST is not string or dict.'
        url = urljoin(connection.server, collection_name)
        logging.debug("POST URL : %s" % (url))
        logging.debug("POST data: %s" % (json.dumps(post_input,
                                         sort_keys=True, indent=4,
                                         separators=(',', ': '))))
        response = requests.post(url, auth=connection.auth,
                                 headers=connection.headers, data=json_payload)
        logging.debug("POST RESPONSE: %s" % (json.dumps(response.json(),
                                             indent=4, separators=(',', ': '))))
        if not response.status_code == 201:
            logging.warning('POST failure. Response = %s' % (response.text))
        logging.debug("Return object: %s" % (json.dumps(response.json(),
                                             sort_keys=True, indent=4,
                                             separators=(',', ': '))))
        return response.json()

    def fastq_read(self, connection, uri=None, filename=None, reads=1):
        '''Read a few fastq records
        '''
        # https://github.com/detrout/encode3-curation/blob/master/validate_encode3_aliases.py#L290
        # originally written by Diane Trout

        # Reasonable power of 2 greater than 50 + 100 + 5 + 100
        # which is roughly what a single fastq read is.
        if uri:
            BLOCK_SIZE = 512
            url = urljoin(connection.server, quote(uri))
            data = requests.get(url, auth=connection.auth, stream=True)
            block = BytesIO(next(data.iter_content(BLOCK_SIZE * reads)))
            compressed = gzip.GzipFile(None, 'r', fileobj=block)
        elif filename:
            compressed = gzip.GzipFile(filename, 'r')
        else:
            print "No url or filename provided! Cannot access file!"
            return
        for i in range(reads):
            header = compressed.readline().rstrip()
            sequence = compressed.readline().rstrip()
            qual_header = compressed.readline().rstrip()
            quality = compressed.readline().rstrip()
            yield (header, sequence, qual_header, quality)


def main():
    keyfile = raw_input("keyfile: ")
    keyname = raw_input("key: ")
    infile = raw_input("infile: ")
    key = ENC_Key(keyfile, keyname)
    connection = ENC_Connection(key)
    print "Running on", connection.server
    with open(infile, "r") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            newF = NewFile(row, connection)
            newF.post_file()
            print "Data to POST: ", newF.post_input

if __name__ == '__main__':
    main()
