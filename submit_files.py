import hashlib
import os.path
import subprocess
import time
import sys
import csv
import encodedcc
import argparse

#############################
# Set defaults


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--infile',
                        help="TSV file with data, needs headers")
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
                        help="Allows script to update")
    args = parser.parse_args()
    return args


class NewFile():
    def __init__(self, dictionary):
        self.data = dictionary
        self.post_input = {}
        for key in dictionary.keys():
            if dictionary.get(key):
                self.post_input[key] = dictionary[key]

    def post_file(self, connection):

        ####################
        # POST metadata
        print("Submitting metadata.")
        r = encodedcc.new_ENCODE(connection, "/files/", self.post_input)
        item = r["@graph"][0]
        #####################
        # POST file to S3

        creds = item['upload_credentials']
        env = os.environ.copy()
        env.update({
            'AWS_ACCESS_KEY_ID': creds['access_key'],
            'AWS_SECRET_ACCESS_KEY': creds['secret_key'],
            'AWS_SECURITY_TOKEN': creds['session_token'],
        })
        print("Uploading file.")
        print(self.file_path)
        start = time.time()
        subprocess.check_call(['aws', 's3', 'cp', self.file_path, creds['upload_url']], env=env)
        end = time.time()
        duration = end - start
        print("Uploaded in %.2f seconds" % duration)

######################
# Main


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on", connection.server)
    if args.update:
        print("This is an UPDATE run, data will be changed")
    else:
        print("This is a TEST run, nothing gets altered")
    with open(args.infile, "r") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            n = NewFile(row)
            if args.update:
                n.post_file(connection)
            else:
                print("Data to POST: ", n.post_input)

if __name__ == '__main__':
    main()
