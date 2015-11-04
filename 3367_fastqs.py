import requests
import gzip
from io import BytesIO
import argparse
import os.path
import encodedcc
from urllib.parse import urljoin
from urllib.parse import quote

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    parser.add_argument("--accession",
                        help="accession of fastq file to check")
    args = parser.parse_args()
    return args

# taken from https://github.com/detrout/encode3-curation/blob/master/validate_encode3_aliases.py#L290
# originally written by Diane Trout


def fastq_read(uri, connection, reads=1):
    '''Read a few fastq records
    '''
    # Reasonable power of 2 greater than 50 + 100 + 5 + 100
    # which is roughly what a single fastq read is.
    BLOCK_SIZE = 512
    url = urljoin(connection.server, quote(uri))
    data = requests.get(url, auth=connection.auth, stream=True)

    block = BytesIO(next(data.iter_content(BLOCK_SIZE * reads)))
    compressed = gzip.GzipFile(None, 'r', fileobj=block)
    for i in range(reads):
        header = compressed.readline().rstrip()
        sequence = compressed.readline().rstrip()
        qual_header = compressed.readline().rstrip()
        quality = compressed.readline().rstrip()
        yield (header, sequence, qual_header, quality)


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on", connection.server)
    if args.accession:
        accession = args.accession
    else:
        accession = "ENCFF295WRQ"
    uri = "/files/" + accession + "/@@download/" + accession + ".fastq.gz"
    print("header\tsequence\tread length")
    for header, sequence, qual_header, quality in fastq_read(uri, connection):
        header = header.decode("UTF-8")
        sequence = sequence.decode("UTF-8")
        print(header + "\t" + sequence + "\t" + str(len(sequence)))

if __name__ == '__main__':
        main()
