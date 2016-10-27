import argparse
import os
import encodedcc
import logging
import sys

EPILOG = '''
%(prog)s is a script that will extract uuid(s) of all the objects
that are linked to the dataset accession given as input


Basic Useage:

    %(prog)s --infile file.txt
    %(prog)s --infile ENCSR000AAA
    %(prog)s --infile ENCSR000AAA,ENCSR000AAB,ENCSR000AAC

    A single column file listing the  identifiers of the objects desired
    A single identifier or comma separated list of identifiers is also useable


Misc. Useage:

    The output file default is 'Extract_report.txt'
    This can be changed with '--output'

    Default keyfile location is '~/keyfile.json'
    Change with '--keyfile'

    Default key is 'default'
    Change with '--key'

'''

logger = logging.getLogger(__name__)


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--infile',
                        help="File containing single column of accessions " +
                        "or a single accession or comma separated list " +
                        "of accessions")
    parser.add_argument('--outfile',
                        help="Output file name", default='Release_report.txt')
    parser.add_argument('--key',
                        help="The keypair identifier from the keyfile.",
                        default='default')
    parser.add_argument('--keyfile',
                        help="The keyfile",
                        default=os.path.expanduser('~/keypairs.json'))
    parser.add_argument('--debug',
                        help="Run script in debug mode.  Default is off",
                        action='store_true', default=False)

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(levelname)s:%(message)s',
                            level=logging.DEBUG)
    else:  # use the default logging level
        logging.basicConfig(filename=args.outfile, filemode="w",
                            format='%(message)s',
                            level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return args

class Data_Extract():
    def __init__(self, args, connection):
        self.extractor_version = 1.0
        self.infile = args.infile
        self.outfile = args.outfile
        self.keysLink = []
        self.PROFILES = {}
        #self.ACCESSIONS = []
        self.connection = connection
        temp = encodedcc.get_ENCODE("/profiles/", self.connection)

        self.profilesJSON = []
        for profile in temp.keys():
            self.profilesJSON.append(profile)
        self.profiles_ref = []
        for profile in self.profilesJSON:
            self.profiles_ref.append(self.helper(profile))
        for item in self.profilesJSON:
            profile = temp[item]  # getting the whole schema profile
            self.keysLink = []  # if a key is in this list, it points to a
            # link and will be embedded in the final product
            
            object_type = item
            #print (object_type)
            self.make_profile(profile, object_type)
            #print (self.keysLink)
            self.PROFILES[item] = self.keysLink
            #print ('--------------')

    def helper(self, item):
        '''feed this back to making references between official object name \
        and the name used in things like the @id
        such as Library and libraries'''
        if item.endswith("y"):
            # this is a hack to find words like "Library"
            item = item.rstrip("y") + "ies"
        elif item.endswith("s"):
            # check for things with "Series" type endings
            pass
        else:
            item = item + "s"
        return item.lower()

    def make_profile(self, dictionary, object_type):
        '''builds the PROFILES reference dictionary
        keysLink is the list of keys that point to links,
        used in the PROFILES'''
        d = dictionary["properties"]
        for prop in d.keys():
                if d[prop].get("linkTo") or d[prop].get("linkFrom"):
                    self.keysLink.append(prop)
                else:
                    if d[prop].get("items"):
                            i = d[prop].get("items")
                            if i.get("linkFrom") or i.get("linkTo"):
                                self.keysLink.append(prop)

    def set_up(self):
        '''do some setup for script'''
        if self.infile:
            if os.path.isfile(self.infile):
                self.ACCESSIONS = [line.rstrip('\n') for line in open(
                    self.infile)]
            else:
                self.ACCESSIONS = self.infile.split(",")
        if len(self.ACCESSIONS) == 0:
            # if something happens and we end up with no accessions stop
            print("ERROR: object has no identifier", file=sys.stderr)
            sys.exit(1)

    def run_script(self):
        # set_up() gets all the command line arguments and validates them
        # also makes the list of accessions to run from
        self.set_up()


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print ("Running on", key.server)
    extract = Data_Extract(args, connection)
    extract.run_script()


if __name__ == "__main__":
    main()
