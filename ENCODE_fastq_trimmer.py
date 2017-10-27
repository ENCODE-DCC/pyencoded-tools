import argparse
import os.path
#import encodedcc
import sys
import requests
from urllib.parse import urljoin
import gzip
import subprocess
import csv
import logging

logger = logging.getLogger(__name__)

EPILOG = '''
This script will trim fastqs down to a read length specified in the TSV file
with the '--object' option

TSV format:
The TSV requires the accession and read_length of the files you want edited
If the run_type needs to be changed include the run_type
Include paired_with on the second file in the pair

EX:
accession   read_length run_type        paired_with
ENCFF240OHP 25          paired-ended
ENCFF521WIC 25          paired-ended    ENCFF240OHP

This is a dryrun-default script and needs to be run with '--update' for
changes to occur

please note that this script assumes that the files are valid and uploads
them without running validateFile unlke ENCODE_submit_files.py

If you want the files validated or you simply want to run them through
ENCODE_submit_files then run without the '--update' option to make new files


For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--object',
                        help="TSV file of accessions, desired read length and \
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


def trim_file(url, connection, filename, size):
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
                # snip off newline, trim to size, add back newline
                quality = next(gzfile)[:-1][:size] + b'\n'
                outfile.write(header)
                outfile.write(sequence)
                outfile.write(qual_header)
                outfile.write(quality)
            except StopIteration:
                break


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    data = []
    if args.update:
        print("This is an UPDATE run.  Data will be POSTed to server")
    else:
        print("This is a dry run.  Nothing will be changed.")
    if os.path.isfile(args.object):
        with open(args.object, "r") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter="\t")
            for row in reader:
                data.append(row)
    else:
        print("No file provided!")
    if len(data) == 0:
        print("No data to check!", file=sys.stderr)
        sys.exit(1)
    for line in data:
        acc = line["accession"]
        size = int(line["read_length"])

        filename = acc + "_modified.fastq.gz"
        link = "/files/" + acc + "/@@download/" + acc + ".fastq.gz"
        url = urljoin(connection.server, link)
        trim_file(url, connection, filename, size)

        print("making metadata")
        file_data = encodedcc.get_ENCODE(acc, connection, frame="edit")
        derived_from = "/files/{}/".format(file_data["accession"])
        file_data["derived_from"] = [derived_from]

        unsubmittable = ['md5sum', 'quality_metrics', 'file_size',
                         'schema_version', 'accession', 'date_created',
                         'content_md5sum', 'status', 'submitted_by',
                         'alternate_accessions']
        for item in unsubmittable:
            # remove items we don't want to submit
            file_data.pop(item, None)

        # alter values of some items
        file_data["submitted_file_name"] = filename
        file_data["read_length"] = size
        file_data["aliases"] = [
            "j-michael-cherry:{acc}-{size}".format(acc=acc, size=size)]

        # conditional items, only change under particular circumstances
        if line.get("run_type"):
            file_data["run_type"] = line["run_type"]
            if line["run_type"] == "single-ended":
                file_data.pop("paired_end", None)

        if line.get("paired_with"):
            # file has partner, time for fancy stuff
            pair = "j-michael-cherry:{acc}-{size}".format(
                acc=line["paired_with"], size=size)
            file_data["paired_with"] = pair
        print(file_data)

        # upload file to ENCODE
        file_object = encodedcc.post_file(file_data, connection, args.update)
        if not file_object:
            logger.warning('Skipping row %d: POST file object failed' % (acc))
            continue
        # post on aws
        aws_return_code = encodedcc.upload_file(file_object, args.update)
        if aws_return_code:
            logger.warning(
                'Row %d: Non-zero AWS upload return code %d' % (aws_return_code))

        if args.update:
            # remove file because space reasons
            print("Removing file {filename}".format(filename=filename))
            subprocess.call(["rm", "{filename}".format(filename=filename)])


if __name__ == '__main__':
    main()
