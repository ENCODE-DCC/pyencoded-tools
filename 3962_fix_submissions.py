import argparse
import os.path
import encodedcc
import sys
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
    search = "/search/?type=Experiment&lab.title=Bradley+Bernstein%2C+Broad&award.project=ENCODE"
    data = encodedcc.get_ENCODE(search, connection).get("@graph", [])
    headers = ["Experiment Accession", "Experiment Aliases", "File Accession", "File Aliases",
               "Submitted File Name", "Bio Rep Num", "Tech Rep Num", "Replicate Aliases",
               "Library Accession", "Library Aliases", "Date Created"]
    with open("broad_lab.txt", "w") as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        for exp in data:
            temp = dict.fromkeys(headers)
            temp["Experiment Accession"] = exp.get("accession")
            temp["Experiment Aliases"] = exp.get("aliases")
            files_list = exp.get("files")
            for fi in files_list:
                file = encodedcc.get_ENCODE(fi, connection)
                if file.get("file_type", "") == "fastq":
                    temp["File Accession"] = file.get("accession")
                    temp["File Aliases"] = file.get("aliases")
                    temp["Submitted File Name"] = file.get("submitted_file_name")
                    temp["Date Created"] = file.get("date_created")
                    if file.get("replicate"):
                        rep = encodedcc.get_ENCODE(file["replicate"], connection)
                        temp["Replicate Aliases"] = rep.get("aliases")
                        temp["Bio Rep Num"] = rep.get("biological_replicate_number")
                        temp["Tech Rep Num"] = rep.get("technical_replicate_number")
                        if rep.get("library"):
                            lib = encodedcc.get_ENCODE(rep["library"], connection)
                            temp["Library Accession"] = lib.get("accession")
                            temp["Library Aliases"] = lib.get("aliases")
                    writer.writerow(temp)

        

if __name__ == '__main__':
        main()
