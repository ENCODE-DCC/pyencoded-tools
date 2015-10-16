import encodedcc
import argparse
import os
import csv
import decimal


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--query',
                        help="takes the @type you want")
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
    parser.add_argument('--onefield',
                        help='Field to get')
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help="Let the script PATCH the data.  Default is False")
    parser.add_argument('--infile',
                        default='test.tsv',
                        help='IGNORE')
    parser.add_argument('--multifield',
                        help="IGNORE")
    parser.add_argument('--accession',
                        help="IGNORE")
    parser.add_argument('--field',
                        help='IGNORE')
    parser.add_argument('--data',
                        help='IGNORE')
    parser.add_argument('--remove',
                        help="IGNORE")
    parser.add_argument('--alias',
                        help="IGNORE")
    args = parser.parse_args()
    return args


def format_number(num):
    try:
        dec = decimal.Decimal(num)
    except:
        return 'NaN'
    tup = dec.as_tuple()
    delta = len(tup.digits) + tup.exponent
    digits = ''.join(str(d) for d in tup.digits)
    if delta <= 0:
        zeros = abs(tup.exponent) - len(tup.digits)
        val = '0.' + ('0'*zeros) + digits
    else:
        val = digits[:delta] + ('0'*tup.exponent) + '.' + digits[delta:]
    val = val.rstrip('0')
    if val[-1] == '.':
        val = val[:-1]
    if tup.sign:
        return '-' + val
    return val


def main():
    args = getArgs()
    args.query = "/search/?type=" + args.query
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    encodedcc.get_fields(args, connection)
    data = []
    with open("test.tsv", "r") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
                data.append(row)
    for x in data:
        for key in x.keys():
            if key != "accession":
                x[key] = format_number(x[key])
    header = ["accession", args.onefield]
    with open("test.tsv", "w") as tsvfile:
        writer = csv.DictWriter(tsvfile, delimiter='\t', fieldnames=header)
        writer.writeheader()
        for x in data:
            writer.writerow(x)

    encodedcc.patch_set(args, connection)

if __name__ == '__main__':
    main()
