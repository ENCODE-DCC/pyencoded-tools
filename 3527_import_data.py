import argparse
import os.path
import encodedcc
import xlrd
import datetime
import sys

EPILOG = '''
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
                        help="Let the script PATCH the data.  Default is False")
    args = parser.parse_args()
    return args


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


def excel_reader(datafile, sheet, update, connection):
    row = reader(datafile, sheetname=sheet)
    print(sheet)
    keys = next(row)  # grab the first row of headers
    print("headers", keys)
    for values in row:
        post_json = dict(zip(keys, values))
        print(post_json)
        if update:
            print("Updating data!")
            #encodedcc.new_ENCODE(connection, sheet, post_json)


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    if args.type:
        names = [args.type]
    else:
        book = xlrd.open_workbook(args.infile)
        names = book.sheet_names()
    profiles = encodedcc.get_ENCODE("/profiles/", connection)
    supported_collections = list(profiles.keys())
    supported_collections.append("Sheet1")
    supported_collections = [s.lower() for s in supported_collections]
#    supported_collections = [s.lower() for s in list(profiles.keys())]
    for n in names:
        if n.lower() in supported_collections:
            excel_reader(args.infile, n, args.update, connection)
        else:
            print("Sheet name '{}' not part of supported object types!".format(n), file=sys.stderr)

if __name__ == '__main__':
        main()