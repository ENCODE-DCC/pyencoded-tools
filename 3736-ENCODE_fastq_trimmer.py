import argparse
import os.path
import encodedcc
import sys
import requests
from urllib.parse import urljoin
from urllib.parse import quote
import gzip
import subprocess
import csv

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--object',
                        help="CSV file of accessions, desired read length and \
                        other metadata needed for submitting")
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


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    data = []
    if os.path.isfile(args.object):
        with open(args.object, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
    if len(data) == 0:
        print("No data to check!", file=sys.stderr)
        sys.exit(1)
    for line in data:
        acc = line["accession"]
        size = int(line["read_length"])

        filename = acc + "_modified.fastq.gz"
        link = "/files/" + acc + "/@@download/" + acc + ".fastq.gz"
        url = urljoin(connection.server, link)
        r = requests.get(url, auth=connection.auth, stream=True)
        gzfile = gzip.GzipFile(fileobj=r.raw)
        with gzip.open(filename, "wb") as outfile:
            print("writing file {filename}".format(filename=filename))
            while True:
                try:
                    #import pdb; pdb.set_trace()
                    header = next(gzfile)
                    sequence = next(gzfile)[:-1][:size] + b'\n'
                    qual_header = next(gzfile)
                    quality = next(gzfile)[:-1][:size] + b'\n'  # snip off newline, trim to size, add back newline
                    outfile.write(header)
                    outfile.write(sequence)
                    outfile.write(qual_header)
                    outfile.write(quality)
                except StopIteration:
                    break
        if args.update:
            # make tempfile?
            print("making metadata file")
            file_data = encodedcc.get_ENCODE(acc, connection, frame="edit")
            unsubmittable = ['md5sum', 'content_md5sum', 'accession']
            for item in unsubmittable:
                file_data.pop(item, None)
            file_data["submitted_file_name"] = filename
            file_data["read_length"] = size
            if line.get("run_type"):
                file_data["run_type"] = line["run_type"]
                if line["run_type"] == "single-ended":
                    file_data.pop("paired_end", None)
            if line.get("derived_from"):
                file_data["derived_from"] = line["derived_from"]

            headers = list(file_data.keys())
            tempfile = acc + "_meta.csv"
            with open(tempfile, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerow(file_data)

            print("Uploading file {filename}".format(filename=filename))
            subprocess.call("./ENCODE_submit_files.py {infile} --key {key} --update".format(infile=tempfile, key=args.key))
            print("Removing file {filename}".format(filename=filename))
            subprocess.call("rm {filename}".format(filename=filename))


if __name__ == '__main__':
        main()
