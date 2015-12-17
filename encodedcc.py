#!/usr/bin/env python
# -*- coding: latin-1 -*-

import requests
import json
import sys
import logging
from urllib.parse import urljoin
from urllib.parse import quote


class dict_diff(object):
    """
    Calculate items added, items removed, keys same in both but changed values,
    keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.current_keys, self.past_keys = [
            set(d.keys()) for d in (current_dict, past_dict)
        ]
        self.intersect = self.current_keys.intersection(self.past_keys)

    def added(self):
        diff = self.current_keys - self.intersect
        if diff == set():
            return None
        else:
            return diff

    def removed(self):
        diff = self.past_keys - self.intersect
        if diff == set():
            return None
        else:
            return diff

    def changed(self):
        diff = set(o for o in self.intersect
                   if self.past_dict[o] != self.current_dict[o])
        if diff == set():
            return None
        else:
            return diff

    def unchanged(self):
        diff = set(o for o in self.intersect
                   if self.past_dict[o] == self.current_dict[o])
        if diff == set():
            return None
        else:
            return diff

    def same(self):
        return self.added() is None and self.removed() is None and self.changed() is None


class ENC_Key:
    def __init__(self, keyfile, keyname):
        keys_f = open(keyfile, 'r')
        keys_json_string = keys_f.read()
        keys_f.close()
        keys = json.loads(keys_json_string)
        key_dict = keys[keyname]
        self.authid = key_dict['key']
        self.authpw = key_dict['secret']
        self.server = key_dict['server']
        if not self.server.endswith("/"):
            self.server += "/"


class ENC_Connection(object):
    def __init__(self, key):
        self.headers = {'content-type': 'application/json'}
        self.server = key.server
        self.auth = (key.authid, key.authpw)


class ENC_Collection(object):
    def __init__(self, connection, supplied_name, frame='object'):
        if supplied_name.endswith('s'):
            self.name = supplied_name.replace('_', '-')
            self.search_name = supplied_name.rstrip('s').replace('-', '_')
            self.schema_name = self.search_name + '.json'
        elif supplied_name.endswith('.json'):
            self.name = supplied_name.replace('_', '-').rstrip('.json')
            self.search_name = supplied_name.replace('-', '_').rstrip('.json')
            self.schema_name = supplied_name
        else:
            self.name = supplied_name.replace('_', '-') + 's'
            self.search_name = supplied_name.replace('-', '_')
            self.schema_name = supplied_name.replace('-', '_') + '.json'
        schema_uri = '/profiles/' + self.schema_name
        self.connection = connection
        self.server = connection.server
        self.schema = get_ENCODE(schema_uri, connection)
        self.frame = frame
        search_string = '/search/?format=json&limit=all&\
                        type=%s&frame=%s' % (self.search_name, frame)
        collection = get_ENCODE(search_string, connection)
        self.items = collection['@graph']
        self.es_connection = None

    def query(self, query_dict, maxhits=10000):
        from pyelasticsearch import ElasticSearch
        if self.es_connection is None:
            es_server = self.server.rstrip('/') + ':9200'
            self.es_connection = ElasticSearch(es_server)
        results = self.es_connection.search(query_dict, index='encoded',
                                            doc_type=self.search_name,
                                            size=maxhits)
        return results

global schemas
schemas = []


class ENC_Schema(object):
    def __init__(self, connection, uri):
        self.uri = uri
        self.connection = connection
        self.server = connection.server
        response = get_ENCODE(uri, connection)
        self.properties = response['properties']


class ENC_Item(object):
    def __init__(self, connection, id, frame='object'):
        self.id = id
        self.connection = connection
        self.server = connection.server
        self.frame = frame

        if id is None:
            self.type = None
            self.properties = {}
        else:
            if id.rfind('?') == -1:
                get_string = id + '?'
            else:
                get_string = id + '&'
            get_string += 'frame=%s' % (frame)
            item = get_ENCODE(get_string, connection)
            self.type = next(x for x in item['@type'] if x != 'item')
            self.properties = item

    def get(self, key):
        try:
            return self.properties[key]
        except KeyError:
            return None

    def sync(self):
        if self.id is None:  # There is no id, so this is a new object to POST
            excluded_from_post = ['schema_version']
            self.type = self.properties.pop('@type')
            schema_uri = 'profiles/%s.json' % (self.type)
            try:
                schema = next(x for x in schemas if x.uri == schema_uri)
            except StopIteration:
                schema = ENC_Schema(self.connection, schema_uri)
                schemas.append(schema)

            post_payload = {}
            for prop in self.properties:
                if prop in schema.properties and prop not in excluded_from_post:
                    post_payload.update({prop: self.properties[prop]})
                else:
                    pass
            # should return the new object that comes back from the patch
            new_object = new_ENCODE(self.connection, self.type, post_payload)

        else:  # existing object to PATCH or PUT
            if self.id.rfind('?') == -1:
                get_string = self.id + '?'
            else:
                get_string = self.id + '&'
            get_string += 'frame=%s' % (self.frame)
            on_server = get_ENCODE(get_string, self.connection)
            diff = dict_diff(on_server, self.properties)
            if diff.same():
                logging.warning("%s: No changes to sync" % (self.id))
            elif diff.added() or diff.removed():  # PUT
                excluded_from_put = ['schema_version']
                schema_uri = '/profiles/%s.json' % (self.type)
                try:
                    schema = next(x for x in schemas if x.uri == schema_uri)
                except StopIteration:
                    schema = ENC_Schema(self.connection, schema_uri)
                    schemas.append(schema)

                put_payload = {}
                for prop in self.properties:
                    if prop in schema.properties and prop not in excluded_from_put:
                        put_payload.update({prop: self.properties[prop]})
                    else:
                        pass
                # should return the new object that comes back from the patch
                new_object = replace_ENCODE(self.id, self.connection, put_payload)

            else:  # PATCH

                excluded_from_patch = ['schema_version', 'accession', 'uuid']
                patch_payload = {}
                for prop in diff.changed():
                    if prop not in excluded_from_patch:
                        patch_payload.update({prop: self.properties[prop]})
                # should probably return the new object that comes back from the patch
                new_object = patch_ENCODE(self.id, self.connection, patch_payload)

        return new_object

    def new_creds(self):
        if self.type == 'file':  # There is no id, so this is a new object to POST
            r = requests.post("%s/%s/upload/" % (self.connection.server, self.id),
                              auth=self.connection.auth,
                              headers=self.connection.headers,
                              data=json.dumps({}))
            return r.json()['@graph'][0]['upload_credentials']
        else:
            return None


def get_ENCODE(obj_id, connection, frame="object"):
    '''GET an ENCODE object as JSON and return as dict'''
    if '?' in obj_id:
        url = urljoin(connection.server, obj_id+'&limit=all&frame='+frame)
    else:
        url = urljoin(connection.server, obj_id+'?limit=all&frame='+frame)
    logging.debug('GET %s' % (url))
    response = requests.get(url, auth=connection.auth, headers=connection.headers)
    logging.debug('GET RESPONSE code %s' % (response.status_code))
    try:
        if response.json():
            logging.debug('GET RESPONSE JSON: %s' % (json.dumps(response.json(), indent=4, separators=(',', ': '))))
    except:
        logging.debug('GET RESPONSE text %s' % (response.text))
    if not response.status_code == 200:
        logging.warning('GET failure.  Response code = %s' % (response.text))
    return response.json()


def replace_ENCODE(obj_id, connection, put_input):
    '''PUT an existing ENCODE object and return the response JSON
    '''
    if isinstance(put_input, dict):
        json_payload = json.dumps(put_input)
    elif isinstance(put_input, str):
        json_payload = put_input
    else:
        logging.warning('Datatype to PUT is not string or dict.')
    url = urljoin(connection.server, obj_id)
    logging.debug('PUT URL : %s' % (url))
    logging.debug('PUT data: %s' % (json_payload))
    response = requests.put(url, auth=connection.auth, data=json_payload,
                            headers=connection.headers)
    logging.debug('PUT RESPONSE: %s' % (json.dumps(response.json(), indent=4,
                                                   separators=(',', ': '))))
    if not response.status_code == 200:
        logging.warning('PUT failure.  Response = %s' % (response.text))
    return response.json()


def patch_ENCODE(obj_id, connection, patch_input):
    '''PATCH an existing ENCODE object and return the response JSON
    '''
    if isinstance(patch_input, dict):
        json_payload = json.dumps(patch_input)
    elif isinstance(patch_input, str):
        json_payload = patch_input
    else:
        print('Datatype to PATCH is not string or dict.', file=sys.stderr)
    url = urljoin(connection.server, obj_id)
    logging.debug('PATCH URL : %s' % (url))
    logging.debug('PATCH data: %s' % (json_payload))
    response = requests.patch(url, auth=connection.auth, data=json_payload,
                              headers=connection.headers)
    logging.debug('PATCH RESPONSE: %s' % (json.dumps(response.json(), indent=4,
                                                     separators=(',', ': '))))
    if not response.status_code == 200:
        logging.warning('PATCH failure.  Response = %s' % (response.text))
    return response.json()


def new_ENCODE(connection, collection_name, post_input):
    '''POST an ENCODE object as JSON and return the response JSON
    '''
    if isinstance(post_input, dict):
        json_payload = json.dumps(post_input)
    elif isinstance(post_input, str):
        json_payload = post_input
    else:
        print('Datatype to POST is not string or dict.', file=sys.stderr)
    url = urljoin(connection.server, collection_name)
    logging.debug("POST URL : %s" % (url))
    logging.debug("POST data: %s" % (json.dumps(post_input,
                                     sort_keys=True, indent=4,
                                     separators=(',', ': '))))
    response = requests.post(url, auth=connection.auth,
                             headers=connection.headers, data=json_payload)
    logging.debug("POST RESPONSE: %s" % (json.dumps(response.json(),
                                         indent=4, separators=(',', ': '))))
    if not response.status_code == 201:
        logging.warning('POST failure. Response = %s' % (response.text))
    logging.debug("Return object: %s" % (json.dumps(response.json(),
                                         sort_keys=True, indent=4,
                                         separators=(',', ': '))))
    return response.json()


def flat_one(JSON_obj):
    try:
        return [JSON_obj[identifier] for identifier in
                ['accession', 'name', 'email', 'title', 'uuid', 'href']
                if identifier in JSON_obj][0]
    except:
        return JSON_obj


def flat_ENCODE(JSON_obj):
    flat_obj = {}
    for key in JSON_obj:
        if isinstance(JSON_obj[key], dict):
            flat_obj.update({key: flat_one(JSON_obj[key])})
        elif isinstance(JSON_obj[key], list) and JSON_obj[key] != [] and isinstance(JSON_obj[key][0], dict):
            newlist = []
            for obj in JSON_obj[key]:
                newlist.append(flat_one(obj))
            flat_obj.update({key: newlist})
        else:
            flat_obj.update({key: JSON_obj[key]})
    return flat_obj


def pprint_ENCODE(JSON_obj):
    if ('type' in JSON_obj) and (JSON_obj['type'] == "object"):
        print(json.dumps(JSON_obj['properties'],
                         sort_keys=True, indent=4, separators=(',', ': ')))
    else:
        print(json.dumps(flat_ENCODE(JSON_obj),
                         sort_keys=True, indent=4, separators=(',', ': ')))


def small_func(f, result, last, newObj, header):
    if result.get(last):
        name = f
        if type(result[last]) == int:
            name = name + ":int"
        elif type(result[last]) == list:
            name = name + ":list"
        elif type(result[last]) == dict:
            name = name + ":dict"
        else:
            # this must be a string
            pass
        newObj[name] = result[last]
        if name not in header:
            header.append(name)


def get_fields(args, connection, facet=None):
    ''' facet contains a list with the first itme being a list
    of the accessions and the second a list of the fieldnames '''
    import csv
    accessions = []
    if facet:
        accessions = facet[0]
        fields = facet[1]
    else:
        if args.query:
            if "search" in args.query:
                temp = get_ENCODE(args.query, connection).get("@graph", [])
                for obj in temp:
                    if obj.get("accession"):
                        accessions.append(obj["accession"])
            else:
                accessions = [get_ENCODE(args.query, connection).get("accession")]
        elif args.infile:
            accessions = [line.strip() for line in open(args.infile)]
        elif args.accession:
            accessions = [args.accession]
        else:
            print("ERROR: Need to provide accessions")
            sys.exit(1)

        if args.multifield:
            fields = [line.strip() for line in open(args.multifield)]
        elif args.onefield:
            fields = [args.onefield]
        else:
            print("ERROR: Need to provide fields!")
            sys.exit(1)
    data = {}
    header = []
    if "accession" not in fields:
        header = ["accession"]
    if any(accessions) and any(fields):
        for a in accessions:
            a = quote(a)
            result = get_ENCODE(a, connection)
            newObj = {}
            for f in fields:
                full = f.split(".")  # check to see if someone wants embedded value
                print("full=", full)
                for x in full[:-1]:  # cycle through the list except last element
                    print("x=", x)
                    if result.get(x):  # check to see if the element is in the current object
                        #print(type(result[x]))
                        if type(result[x]) == int:
                            pass
                        elif type(result[x]) == list:  # if we have a list of embedded objects we need to cycle through?
                            if facet:
                                print("list", result[x][0])
                                temp = get_ENCODE(result[x][0], connection)
                                result = temp
                            else:
                                print("list")
                                print(x)  # maybe we can use small_func in a loop when we get results from here?
                        elif type(result[x]) == dict:
                            pass
                        else:
                            print("string", result[x])
                            temp = get_ENCODE(result[x], connection)  # if found get_ENCODE the embedded object
                            result = temp
                    #print(temp)
                    #result = temp
                last = full[-1]  # get the last element in the split list
                print("last", last)
                if result.get(last):  # after the above loop, should be at correct depth level to get normal name of field ex. target.name
                    name = f
                    if not facet:
                        #print("NAMENAMENAME", result[last])
                        name = name + get_type(result[last])
                    newObj[name] = result[last]
                    print("new Object", newObj)
                    if name not in header:
                        header.append(name)
            if "accession" not in fields:
                newObj["accession"] = a
            data[a] = newObj
    #print("HIHIHIHIHIHIHIHI", data)
    if facet:
        return data
    else:
        writer = csv.DictWriter(sys.stdout, delimiter='\t', fieldnames=header)
        writer.writeheader()
        for key in data.keys():
            writer.writerow(data.get(key))


def get_type(obj):
    if type(obj) == int:
        return ":int"
    elif type(obj) == list:
        return ":list"
    elif type(obj) == dict:
        return ":dict"
    else:
        # this must be a string
        return ""


def patch_set(args, connection):
    import csv
    data = []
    print("Running on", connection.server)
    if args.update:
        print("This is an UPDATE run, data will be patched")
        if args.remove:
            print("On this run data will be REMOVED")
    else:
        print("This is a test run, nothing will be changed")
    if args.accession:
        if args.field and args.data:
            data.append({"accession": args.accession, args.field: args.data})
        else:
            print("Missing field/data! Cannot PATCH object", args.accession)
            sys.exit(1)
    elif args.infile:
        with open(args.infile, "r") as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t')
            for row in reader:
                data.append(row)
    else:
        reader = csv.DictReader(sys.stdin, delimiter='\t')
        for row in reader:
            data.append(row)
    for d in data:
        accession = d.get("accession")
        if not accession:
            print("Missing accession!  Cannot PATCH data")
            sys.exit(1)
        temp_data = d
        temp_data.pop("accession")
        patch_data = {}
        for key in temp_data.keys():
            k = key.split(":")
            if len(k) > 1:
                if k[1] == "int":
                    patch_data[k[0]] = int(temp_data[key])
                elif k[1] == "list":
                    l = temp_data[key].strip("[]").split(",")
                    l = [x.replace(" ", "") for x in l]
                    if args.overwrite:
                        patch_data[k[0]] = l
                    else:
                        append_list = get_ENCODE(accession, connection).get(k[0], [])
                        print(append_list)
                        patch_data[k[0]] = l + append_list
            else:
                patch_data[k[0]] = temp_data[key]
        accession = quote(accession)
        full_data = get_ENCODE(accession, connection, frame="edit")
        old_data = {}
        for key in patch_data.keys():
            old_data[key] = full_data.get(key)
        if args.remove:
            if args.update:
                put_dict = full_data
                for key in patch_data.keys():
                    put_dict.pop(key, None)
                replace_ENCODE(accession, connection, put_dict)
            print("OBJECT:", accession)
            print("Removing values", str(patch_data.keys()))
        else:
            if args.update:
                patch_ENCODE(accession, connection, patch_data)
            print("OBJECT:", accession)
            for key in patch_data.keys():
                print("OLD DATA:", key, old_data[key])
                print("NEW DATA:", key, patch_data[key])


def fastq_read(connection, uri=None, filename=None, reads=1):
    '''Read a few fastq records
    '''
    # https://github.com/detrout/encode3-curation/blob/master/validate_encode3_aliases.py#L290
    # originally written by Diane Trout
    import gzip
    from io import BytesIO
    # Reasonable power of 2 greater than 50 + 100 + 5 + 100
    # which is roughly what a single fastq read is.
    if uri:
        BLOCK_SIZE = 512
        url = urljoin(connection.server, quote(uri))
        data = requests.get(url, auth=connection.auth, stream=True)
        block = BytesIO(next(data.iter_content(BLOCK_SIZE * reads)))
        compressed = gzip.GzipFile(None, 'r', fileobj=block)
    elif filename:
        compressed = gzip.GzipFile(filename, 'r')
    else:
        print("No url or filename provided! Cannot access file!")
        return
    for i in range(reads):
        header = compressed.readline().rstrip()
        sequence = compressed.readline().rstrip()
        qual_header = compressed.readline().rstrip()
        quality = compressed.readline().rstrip()
        yield (header, sequence, qual_header, quality)
