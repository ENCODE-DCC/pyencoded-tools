import argparse
import os.path
import encodedcc
import datetime

GET_HEADERS = {'accept': 'application/json'}


EPILOG = '''
For more details:

        %(prog)s --help
'''
'''
Example command:
python3 ENCODE_replaced_cleaner.py --keyfile keypairs.json --key test
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    args = parser.parse_args()
    return args


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    software_versions = encodedcc.get_ENCODE('search/?type=SoftwareVersion',
                                             connection)['@graph']
    for soft_version in software_versions:
        print (soft_version['uuid'])
        al = 'encode:' + str(datetime.datetime.now().time())
        new_aliases = soft_version.get('aliases')
        new_aliases.append(al)
        encodedcc.patch_ENCODE(
            soft_version['uuid'],
            connection,
            {"aliases": new_aliases})


    users = encodedcc.get_ENCODE('search/?type=User',
                                 connection)['@graph']
    for user in users:
        print (user['uuid'])
        al = ' encode:' + str(datetime.datetime.now().time())
        name = str(user.get('title')) + al
        encodedcc.patch_ENCODE(
            user['uuid'],
            connection,
            {"first_name": name})



if __name__ == '__main__':
    main()
