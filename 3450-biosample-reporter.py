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

    parser.add_argument('--infile',
                        help="single column list of object accessions")
    parser.add_argument('--query',
                        help="query of objects you want to process")
    parser.add_argument('--accession',
                        help="single accession to process")
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
    headers = ["accession", "description", "organism", "age_display",
               "life_stage", "sex", "biosample_term_name", "biosample_type",
               "depleted_in_term_name", "phase",
               "subcellular_fraction_term_name", "post_synchronization_time",
               "post_synchronization_time_units", "synchronization",
               "model_organism_mating_status", "treatments", "donor",
               "transfection_type", "talens", "constructs",
               "model_organism_donor_constructs", "rnais", "part_of",
               "pooled_from", "derived_from", "status", "culture_harvest_date",
               "culture_start_date", "date_obtained", "lab", "source", "note",
               "notes", "health_status", "starting_amount",
               "starting_amount_units"]
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    accessions = []
    if args.query:
        temp = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        for obj in temp:
            accessions.append(obj.get("accession"))
    elif args.infile:
        accessions = [line.strip() for line in open(args.infile)]
    elif args.accession:
        accessions = [args.accession]
    else:
        print("No accessions to check!", file=sys.stderr)
        sys.exit(1)
    data = []
    for acc in accessions:
        temp = {}
        obj = encodedcc.get_ENCODE(acc, connection)
        for h in headers:
            x = obj.get(h, "")
            if any(x):
                temp[h] = x
            else:
                temp[h] = ""
        data.append(temp)
    writer = csv.DictWriter(sys.stdout, delimiter='\t', fieldnames=headers)
    writer.writeheader()
    for d in data:
        writer.writerow(d)

if __name__ == '__main__':
        main()
