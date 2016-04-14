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
    query = "/search/?type=Experiment&lab.title=Brenton+Graveley%2C+UConn&award.project=ENCODE&status=released&files.file_type=bam"
    data = encodedcc.get_ENCODE(query, connection).get("@graph", [])
    headers = ["File Accession", "Annotation", "Cell Line", "Target", "Experiment Accession", "Experiment Aliases",
               "Biosample Accession", "Biosample Aliases", "Library Accession", "Library Aliases", "Lab", "Submitted Name"]
    with open("output.txt", "w") as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        for exp in data:
            if exp.get("possible_controls"):
                print("Experiment", exp.get("accession"))
                temp = dict.fromkeys(headers)
                temp["Experiment Accession"] = exp.get("accession")
                temp["Experiment Aliases"] = exp.get("aliases")
                temp["Cell Line"] = exp.get("biosample_term_name")
                temp["Target"] = exp.get("target")
                if exp.get("files"):
                    files = exp["files"]
                else:
                    files = exp["original_files"]
                for f in files:
                    file = encodedcc.get_ENCODE(f, connection)
                    if file.get("file_format", "") == "bam":
                        # this is a bam file and we want it
                        temp["Lab"] = file.get("lab")
                        temp["Annotation"] = file.get("genome_annotation")
                        temp["File Accession"] = file.get("accession")
                        temp["Submitted Name"] = file.get("submitted_file_name")
                        print("File", file.get("accession"))
                        if file.get("replicate"):
                            rep = encodedcc.get_ENCODE(file["replicate"], connection)
                            if rep.get("library"):
                                lib = encodedcc.get_ENCODE(rep["library"], connection)
                                temp["Library Accession"] = lib.get("accession")
                                temp["Library Aliases"] = lib.get("aliases")
                                print("Library", lib.get("accession"))
                                if lib.get("biosample"):
                                    bio = encodedcc.get_ENCODE(lib["biosample"], connection)
                                    temp["Biosample Accession"] = bio.get("accession")
                                    temp["Biosample Aliases"] = bio.get("aliases")
                                    print("Biosample", bio.get("accession"))
                        writer.writerow(temp)


if __name__ == '__main__':
        main()
