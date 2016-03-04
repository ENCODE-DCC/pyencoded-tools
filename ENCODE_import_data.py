#!/usr/bin/env python3
# -*- coding: latin-1 -*-
import argparse
import os.path
import encodedcc
import xlrd
import datetime
import sys
import mimetypes
import requests
from PIL import Image
from base64 import b64encode
import magic  # install me with 'pip install python-magic'
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
Ex: Experiment, Biosample, Document, AntibodyCharacterization

    Or use the '--type' argument, but this will only work for single sheet documents
Ex: %(prog)s mydata.xsls --type Experiment


The header of each sheet should be the names of the fields,
Ex: award, lab, target, etc.

To upload objects with attachments, have a column titled "attachment"
containing the name of the file you wish to attach


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
        raise ValueError('Wrong extension for %s: %s' % (detected_type, filename))

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
#        print("before", post_json)
        post_json = dict_patcher(post_json)
        # I think we can add attchments here
        if post_json.get("attachment"):
            attach = attachment(post_json["attachment"])
            post_json["attachment"] = attach
        #print("after", post_json)
        temp = {}
        if post_json.get("uuid"):
            temp = encodedcc.get_ENCODE(post_json["uuid"], connection)
        elif post_json.get("alias"):
            temp = encodedcc.get_ENCODE(post_json["alias"], connection)
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
                print("Object {} already exists.  Would you like to patch it instead?".format(temp["uuid"]))
                i = input("PATCH? y/n ")
                if i.lower() == "y":
                    e = encodedcc.patch_ENCODE(temp["uuid"], connection, post_json)
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


def dict_patcher(old_dict):
    new_dict = {}
    for key in old_dict.keys():
        if old_dict[key] != "":  # this removes empty cells
            k = key.split(":")
            if len(k) > 1:
                if k[1] == "int" or "integer":
                    new_dict[k[0]] = int(old_dict[key])
                elif k[1] == "list" or k[1] == "array":
                    l = old_dict[key].strip("[]").split(",")
                    #l = [x.replace(" ", "") for x in l]
                    new_dict[k[0]] = l
            else:
                new_dict[k[0]] = old_dict[key]
    return new_dict


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
            excel_reader(args.infile, n, args.update, connection, args.patchall)
        else:
            print("Sheet name '{name}' not part of supported object types!".format(name=n), file=sys.stderr)

if __name__ == '__main__':
        main()
