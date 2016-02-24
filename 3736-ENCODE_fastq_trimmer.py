import argparse
import os.path
import encodedcc
import sys
import requests
from urllib.parse import urljoin
from urllib.parse import quote
import gzip
import subprocess

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
                        help="Either the file containing a list of ENCs as a column,\
                        a single accession by itself, or a comma separated \
                        list of identifiers")
    parser.add_argument('--infile',
                        help="infile for ENCODE_submit_files.py, needs to be CSV \
                        with file metadata for submitting")
    parser.add_argument('--query',
                        help="query of objects you want to process")
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
    accessions = []
    if args.update and not os.path.isfile(args.infile):
        print("Need metadata file for submitting!")
        sys.exit(1)
    if args.query:
        if "search" in args.query:
            temp = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        else:
            temp = [encodedcc.get_ENCODE(args.query, connection)]
        if any(temp):
            for obj in temp:
                if obj.get("accession"):
                    accessions.append(obj["accession"])
                elif obj.get("uuid"):
                    accessions.append(obj["uuid"])
                elif obj.get("@id"):
                    accessions.append(obj["@id"])
                elif obj.get("aliases"):
                    accessions.append(obj["aliases"][0])
                else:
                    print("ERROR: object has no identifier", file=sys.stderr)
    elif args.object:
        if os.path.isfile(args.object):
            accessions = [line.strip() for line in open(args.object)]
        else:
            accessions = [args.object]
    if len(accessions) == 0:
        print("No accessions to check!", file=sys.stderr)
        sys.exit(1)
    for line in accessions:
        acc, size = line.split(",")
        filename = acc + ".fastq.gz"
        link = "/files/" + acc + "/@@download/" + filename
        url = urljoin(connection.server, link)
        r = requests.get(url, auth=connection.auth, stream=True)
        gzfile = gzip.GzipFile(fileobj=r.raw)
        with gzip.open(filename, "wb") as outfile:
            print("writing file {filename}".format(filename=filename))
            while True:
                try:
                    #import pdb; pdb.set_trace()
                    header = next(gzfile)
                    sequence = next(gzfile)[:-1][:int(size)] + b'\n'
                    qual_header = next(gzfile)
                    quality = next(gzfile)[:-1][:int(size)] + b'\n'  # snip off newline, trim to size, add back newline
                    outfile.write(header)
                    outfile.write(sequence)
                    outfile.write(qual_header)
                    outfile.write(quality)
                except StopIteration:
                    break
        if args.update:
            subprocess.call("./ENCODE_submit_files.py {infile} --key {key} --update".format(infile=args.infile, key=args.key))
            subprocess.call("rm {filename}".format(filename=filename))

if __name__ == '__main__':
        main()
