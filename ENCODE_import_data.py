#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os.path
import encodedcc
import json
import xlrd
import datetime
import sys
import mimetypes
import requests
from PIL import Image  # install me with 'pip3 install Pillow'
from urllib.parse import quote
from base64 import b64encode
import magic  # install me with 'pip3 install python-magic'
# https://github.com/ahupp/python-magic
# this is the site for python-magic in case we need it

EPILOG = '''
This script takes in an Excel file with the data
This is a dryrun-default script, run with --update or --patchall to work

By DEFAULT:
If there is a uuid, alias, @id, or accession in the document
it will ask if you want to PATCH that object
Use '--patchall' if you want to patch ALL objects in your document and ignore that message

If no object identifiers are found in the document you need to use '--update'
for POSTing to occur

Defining Object type:
    Name each "sheet" of the excel file the name of the object type you are using
    with the format used on https://www.encodeproject.org/profiles/
Ex: Experiment, Biosample, Document, AntibodyCharacterization

    Or use the '--type' argument, but this will only work for single sheet documents
Ex: %(prog)s mydata.xsls --type Experiment


The header of each sheet should be the names of the fields just as in ENCODE_patch_set.py,
Ex: award, lab, target, etc.

    For integers use ':int' or ':integer'
    For lists use ':list' or ':array'
    String are the default and do not require an identifier


To upload objects with attachments, have a column titled "attachment"
containing the name of the file you wish to attach

FOR EMBEDDED SUBOBJECTS:
Embedded objects are considered to be things like:
 - characterization_reviews in AntibodyCharacterizations
 - tags in Constructs
 They are assumed to be of the format of a list of dictionary objects
 Ex:
 "characterization_reviews": [
        {
            "organism": "/organisms/human/",
            "biosample_term_name": "A375",
            "lane": 2,
            "biosample_type": "immortalized cell line",
            "biosample_term_id": "EFO:0002103",
            "lane_status": "compliant"
        },
        {
            "organism": "/organisms/mouse/",
            "biosample_term_name": "embryonic fibroblast",
            "lane": 3,
            "biosample_type": "primary cell",
            "biosample_term_id": "CL:2000042",
            "lane_status": "compliant"
        }
    ]

Formatting in the document should be as follows for the above example:
characterization_reviews.organism    characterization_reviews.lane:int    ....    characterization_reviews.organism-1    characterization_reviews.lane-1:int
/organisms/human/                    2                                            /organisms/mouse/                      3


REMEMBER:
to define multiple embedded items the number tag comes at the end 
of the object but before the object type, such as object.subobject-N:type
    tags.name    tags.location    tags.name-1    tags.location-1
    FLAG         C-terminal       BOGUS          Fake-data

Again, this will become
"tags": [
        {
            "location": "C-terminal",
            "name": "FLAG"
        },
        {
            "location": "Fake-data",
            "name": "BOGUS"
        }
    ],

For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('infile',
                        help="the datafile containing object data to import")
    parser.add_argument('--type',
                        help="the type of the objects to import")
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
                        help="Let the script PATCH the data.  Default is False"),
    parser.add_argument('--patchall',
                        default=False,
                        action='store_true',
                        help="PATCH existing objects.  Default is False \
                        and will only PATCH with user override")
    args = parser.parse_args()
    return args


def attachment(path):
    """ Create an attachment upload object from a filename
    Embeds the attachment as a data url.
    """
    if not os.path.isfile(path):
        r = requests.get(path)
        path = path.split("/")[-1]
        with open(path, "wb") as outfile:
            outfile.write(r.content)
    filename = os.path.basename(path)
    mime_type, encoding = mimetypes.guess_type(path)
    major, minor = mime_type.split('/')
    detected_type = magic.from_file(path, mime=True).decode('ascii')

    # XXX This validation logic should move server-side.
    if not (detected_type == mime_type or
            detected_type == 'text/plain' and major == 'text'):
        raise ValueError('Wrong extension for %s: %s' %
                         (detected_type, filename))

    with open(path, 'rb') as stream:
        attach = {
            'download': filename,
            'type': mime_type,
            'href': 'data:%s;base64,%s' % (mime_type, b64encode(stream.read()).decode('ascii'))
        }

        if mime_type in ('application/pdf', 'text/plain', 'text/tab-separated-values', 'text/html'):
            # XXX Should use chardet to detect charset for text files here.
            return attach

        if major == 'image' and minor in ('png', 'jpeg', 'gif', 'tiff'):
            # XXX we should just convert our tiffs to pngs
            stream.seek(0, 0)
            im = Image.open(stream)
            im.verify()
            if im.format != minor.upper():
                msg = "Image file format %r does not match extension for %s"
                raise ValueError(msg % (im.format, filename))

            attach['width'], attach['height'] = im.size
            return attach

    raise ValueError("Unknown file type for %s" % filename)


def reader(filename, sheetname=None):
    """ Read named sheet or first and only sheet from xlsx file
    """
    book = xlrd.open_workbook(filename)
    if sheetname is None:
        sheet, = book.sheets()
    else:
        try:
            sheet = book.sheet_by_name(sheetname)
        except xlrd.XLRDError:
            return

    datemode = sheet.book.datemode
    for index in range(sheet.nrows):
        yield [cell_value(cell, datemode) for cell in sheet.row(index)]


def cell_value(cell, datemode):
    ctype = cell.ctype
    value = cell.value

    if ctype == xlrd.XL_CELL_ERROR:
        raise ValueError(repr(cell), 'cell error')

    elif ctype == xlrd.XL_CELL_BOOLEAN:
        return str(value).upper()

    elif ctype == xlrd.XL_CELL_NUMBER:
        if value.is_integer():
            value = int(value)
        return str(value)

    elif ctype == xlrd.XL_CELL_DATE:
        value = xlrd.xldate_as_tuple(value, datemode)
        if value[3:] == (0, 0, 0):
            return datetime.date(*value[:3]).isoformat()
        else:
            return datetime.datetime(*value).isoformat()

    elif ctype in (xlrd.XL_CELL_TEXT, xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
        return value

    raise ValueError(repr(cell), 'unknown cell type')


def data_formatter(value, val_type):
    ''' returns formatted data'''
    if val_type in ["int", "integer"]:
        return int(value)
    elif val_type in ["num", "number"]:
        return float(value)
    elif val_type in ["list", "array"]:
        return value.strip("[]").split(",")
    elif val_type in ["boolean", "bool"]:
        if isinstance(value, bool):
            return value
        elif value in ["True", "TRUE", 1, "1"]:
            return True
        elif value in ["False", "FALSE", 0, "0"]:
            return False
        else:
            raise ValueError('Boolean was expected but got: %s, %s' %
                             (value, type(value)))
    elif val_type in ["json", "object"]:
        return json.loads(value)
    else:
        raise ValueError('Unrecognized type: %s for value: %s' %
                         (val_type, value))


def dict_patcher(old_dict):
    new_dict = {}
    for key in old_dict.keys():
        if old_dict[key] != "":  # this removes empty cells
            k = key.split(":")
            path = k[0].split(".")
            if len(k) == 1 and len(path) == 1:
                # this object is a string and not embedded
                # return plain value
                new_dict[k[0]] = old_dict[key]
            elif len(k) > 1 and len(path) == 1:
                # non-string non-embedded object
                # use data_formatter function
                new_dict[k[0]] = data_formatter(old_dict[key], k[1])
            elif len(k) == 1 and len(path) > 1:
                # embedded string object
                # need to build the mini dictionary to put this in
                value = path[1].split("-")
                if new_dict.get(path[0]):
                    # I have already added the embedded object to the new dictionary
                    # add to it
                    if len(value) > 1:
                        # this has a number next to it
                        if len(new_dict[path[0]]) == int(value[1]):
                            # this means we have not added any part of new item to the list
                            new_dict[path[0]].insert(
                                int(value[1]), {value[0]: old_dict[key]})
                        else:
                            # this should be that we have started putting in the new object
                            new_dict[path[0]][int(value[1])].update(
                                {value[0]: old_dict[key]})
                    else:
                        # the object does not exist in the embedded part, add it
                        new_dict[path[0]][0].update({path[1]: old_dict[key]})
                else:
                    # make new item in dictionary
                    temp_dict = {path[1]: old_dict[key]}
                    new_dict[path[0]] = [temp_dict]
            elif len(k) > 1 and len(path) > 1:
                # embedded non-string object
                # need mini dictionary to build
                value = path[1].split("-")
                if new_dict.get(path[0]):
                    # I have already added the embedded object to the new dictionary
                    # add to it
                    if len(value) > 1:
                        # this has a number next to it
                        if len(new_dict[path[0]]) == int(value[1]):
                            # this means we have not added any part of new item to the list
                            new_dict[path[0]].insert(
                                int(value[1]), {value[0]: old_dict[key]})
                        else:
                            # this should be that we have started putting in the new object
                            new_dict[path[0]][int(value[1])].update(
                                {value[0]: old_dict[key]})
                    else:
                        # the object does not exist in the embedded part, add it
                        new_dict[path[0]][0].update(
                            {path[1]: data_formatter(old_dict[key], k[1])})
                else:
                    # make new item in dictionary
                    temp_dict = {path[1]: data_formatter(old_dict[key], k[1])}
                    new_dict[path[0]] = [temp_dict]
    return new_dict


def excel_reader(datafile, sheet, update, connection, patchall):
    row = reader(datafile, sheetname=sheet)
    keys = next(row)  # grab the first row of headers
    total = 0
    error = 0
    success = 0
    patch = 0
    for values in row:
        total += 1
        post_json = dict(zip(keys, values))
        post_json = dict_patcher(post_json)
        # add attchments here
        if post_json.get("attachment"):
            attach = attachment(post_json["attachment"])
            post_json["attachment"] = attach
        print(post_json)
        temp = {}
        # Silence get_ENCODE failures.
        with encodedcc.print_muted():
            if post_json.get("uuid"):
                temp = encodedcc.get_ENCODE(post_json["uuid"], connection)
            elif post_json.get("aliases"):
                temp = encodedcc.get_ENCODE(quote(post_json["aliases"][0]),
                                            connection)
            elif post_json.get("accession"):
                temp = encodedcc.get_ENCODE(post_json["accession"], connection)
            elif post_json.get("@id"):
                temp = encodedcc.get_ENCODE(post_json["@id"], connection)
        if temp.get("uuid"):
            if patchall:
                e = encodedcc.patch_ENCODE(temp["uuid"], connection, post_json)
                if e["status"] == "error":
                    error += 1
                elif e["status"] == "success":
                    success += 1
                    patch += 1
            else:
                print("Object {} already exists.  Would you like to patch it instead?".format(
                    temp["uuid"]))
                i = input("PATCH? y/n ")
                if i.lower() == "y":
                    e = encodedcc.patch_ENCODE(
                        temp["uuid"], connection, post_json)
                    if e["status"] == "error":
                        error += 1
                    elif e["status"] == "success":
                        success += 1
                        patch += 1
        else:
            if update:
                print("POSTing data!")
                e = encodedcc.new_ENCODE(connection, sheet, post_json)
                if e["status"] == "error":
                    error += 1
                elif e["status"] == "success":
                    success += 1
    print("{sheet}: {success} out of {total} posted, {error} errors, {patch} patched".format(
        sheet=sheet.upper(), success=success, total=total, error=error, patch=patch))


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    print("Running on {server}".format(server=connection.server))
    if not os.path.isfile(args.infile):
        print("File {filename} not found!".format(filename=args.infile))
        sys.exit(1)
    if args.type:
        names = [args.type]
    else:
        book = xlrd.open_workbook(args.infile)
        names = book.sheet_names()
    profiles = encodedcc.get_ENCODE("/profiles/", connection)
    supported_collections = list(profiles.keys())
    supported_collections = [s.lower() for s in list(profiles.keys())]
    for n in names:
        if n.lower() in supported_collections:
            excel_reader(args.infile, n, args.update,
                         connection, args.patchall)
        else:
            print("Sheet name '{name}' not part of supported object types!".format(
                name=n), file=sys.stderr)


if __name__ == '__main__':
    main()
