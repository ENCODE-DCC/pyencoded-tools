import argparse
import os.path
import encodedcc
import sys

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
                        a single accession by itself, or a comma separated list of identifiers")
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
    data = encodedcc.get_ENCODE("/search/?type=Experiment&lab.title=Bradley+Bernstein%2C+Broad&award.project=ENCODE", connection, frame="embedded").get("@graph", [])
    print("file\tstatus\tdate_created")
    for exp in data:
        files = exp.get("files", [])
        file_data = {}
        for f in files:
            if f.get("replicate"):
                rep = f["replicate"]["uuid"]
                date = f["date_created"]
                status = f["status"]
                temp_file = [{"file": f["accession"], "date": date, "status": status}]
                if file_data.get(rep):
                    x = file_data[rep] + temp_file
                else:
                    file_data[rep] = temp_file
        for key in file_data.keys():
            for f in file_data[key]:
                if f["status"] in ["submitted", "uploading", "in progress"]:
                    print("{}\t{}\t{}".format(f["file"], f["status"], f["date"]))





if __name__ == '__main__':
        main()
