import argparse
import os.path
import encodedcc

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
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    args = parser.parse_args()
    return args


def retreive_list_of_replaced(object_to_inspect_acc,
                              connection):
    to_return_list = [object_to_inspect_acc]
    objects_to_inspect = encodedcc.get_ENCODE('search/?type=Item&accession=' +
                                              object_to_inspect_acc,
                                              connection)['@graph']
    if objects_to_inspect:
        for object_to_inspect in objects_to_inspect:
            if object_to_inspect.get('alternate_accessions'):
                for acc in object_to_inspect.get('alternate_accessions'):
                    to_return_list.extend(
                        retreive_list_of_replaced(acc, connection))
                return to_return_list
            else:
                return to_return_list
    else:
        return to_return_list


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    profiles = encodedcc.get_ENCODE('/profiles/', connection)
    for object_type in profiles.keys():
        profile_properties = encodedcc.get_ENCODE(
            '/profiles/' + object_type, connection).get('properties')
        # we should fix only objects that have alternate accessions property
        if profile_properties and profile_properties.get(
                'alternate_accessions'):
            uuid_2_alternate_accessions = {}
            objects = encodedcc.get_ENCODE('search/?type=' + object_type,
                                           connection)['@graph']
            for entry in objects:
                if entry.get('alternate_accessions'):
                    replaced_objects_accessions = []
                    for acc in entry.get('alternate_accessions'):
                        replaced_objects_accessions.extend(
                            retreive_list_of_replaced(acc,
                                                      connection))
                    if sorted(list(set(
                        replaced_objects_accessions))) != sorted(
                       entry.get('alternate_accessions')):
                        uuid_2_alternate_accessions[entry['uuid']] = \
                            set(replaced_objects_accessions)

            for uuid in uuid_2_alternate_accessions.keys():
                uuid_sets_counter = 0
                for key in uuid_2_alternate_accessions.keys():
                    if uuid_2_alternate_accessions[uuid] <= \
                       uuid_2_alternate_accessions[key]:
                        uuid_sets_counter += 1
                if uuid_sets_counter == 1:
                    for acc in list(uuid_2_alternate_accessions[uuid]):
                        to_clean_objects = encodedcc.get_ENCODE(
                            'search/?type=Item&accession=' + acc,
                            connection)['@graph']
                        for object_to_clean in to_clean_objects:
                            print (object_to_clean['uuid'] +
                                   ' alternate accessions list ' +
                                   str(object_to_clean[
                                       'alternate_accessions']) +
                                   ' is removed')
                            encodedcc.patch_ENCODE(
                                object_to_clean['uuid'],
                                connection,
                                {"alternate_accessions": []})

                    print (uuid + ' is patched with ' +
                           str({"alternate_accessions": list(
                                uuid_2_alternate_accessions[uuid])}))
                    encodedcc.patch_ENCODE(
                        uuid,
                        connection,
                        {"alternate_accessions": list(
                            uuid_2_alternate_accessions[uuid])})

if __name__ == '__main__':
    main()
