import argparse
import os.path
import encodedcc

EPILOG = '''
For more details:

        %(prog)s --help
'''


def get_fastq_dictionary(exp, connection):

    controlfiles = {}
    control = encodedcc.get_ENCODE(exp, connection)
    for ff in control['files']:
        if ff.get('file_format') != 'fastq':
            continue
        if 'replicate' not in ff:
            print("Missing replicate error")
            continue
        biorep = str(ff['replicate']['biological_replicate_number'])
        # techrep = str(ff['replicate']['technical_replicate_number'])
        pair = str(ff.get('paired_end'))
        # rep = biorep + '-' + techrep
        # key = rep + '-' + pair
        biokey = biorep + '-' + pair

        if biokey not in controlfiles:
            controlfiles[biokey] = [ff['accession']]
        else:
            print("error: replicate-pair has multiple files")
            controlfiles[biokey].append(ff['accession'])
            # controlfiles[key].append('multiple-files-error')

    return controlfiles


def get_HAIB_fastq_dictionary(controls, connection):

    controlfiles = {}
    for control in controls:
        for ff in control['files']:
            ff_obj = encodedcc.get_ENCODE(ff, connection)
            if ff_obj.get('file_format') != 'fastq':
                continue
            if 'replicate' not in ff_obj:
                print("Missing replicate error")
                continue
            try:
                lib = encodedcc.get_ENCODE(ff_obj['replicate']['library'])
                biosample = lib['biosample']['accession']
            except:
                print('Cannot find biosample')
                biosample = ''

            key = biosample
            if key not in controlfiles:
                controlfiles[key] = [ff_obj['accession']]
            else:
                print("error: biosample has multiple files")
                controlfiles[key].append(ff_obj['accession'])
                controlfiles[key].append('same-biosample-error')

    print(controlfiles)
    return controlfiles


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

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    accessions = []
    if args.query:
        temp = encodedcc.get_ENCODE(args.query, connection).get("@graph", [])
        for obj in temp:
            accessions.append(obj.get("@id"))
    elif args.infile:
        accessions = [line.strip() for line in open(args.infile)]
    elif args.accession:
        accessions = [args.accession]
    else:
        assert args.query or args.infile or args.accession, "No accessions to check!"

    for acc in accessions:
        print("Experiment:", acc)
        obj = encodedcc.get_ENCODE(acc, connection)
        controlfiles = {}
        # Missing possible controls, bail out
        if 'possible_controls' not in obj or len(obj['possible_controls']) == 0:
            print('error: {} has no possible_controls'.format(obj['accession']))
            continue

        # If it is HAIB
        elif obj['lab']['name'] == 'richard-myers':
            controlfiles = get_HAIB_fastq_dictionary(obj['possible_controls'])

        # Single possible control
        elif len(obj['possible_controls']) == 1:
            controlId = obj['possible_controls'][0]['accession']
            controlfiles = get_fastq_dictionary(controlId)

        if args.debug:
            print(controlfiles)

if __name__ == '__main__':
        main()
