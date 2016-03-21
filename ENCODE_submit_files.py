#!/usr/bin/env python3
# -*- coding: latin-1 -*-
'''Take a CSV with file metadata, POST new file objects to the ENCODE DCC,
 upload files to the ENCODE cloud bucket'''

import os
import sys
import logging
from urllib.parse import urljoin
import requests
import csv
import copy
import json
import subprocess
import hashlib
import tempfile
import encodedcc

logger = logging.getLogger(__name__)

EPILOG = '''
Dryrun default script, run with '--update' to make changes
Provide with a CSV file of metadata to post

    %(prog)s --encvaldata ./encValData
Use to define a different location for the encValData directory

    %(prog)s --validatefiles ./validateFiles
use to define a different location for the validateFiles script
validateFiles must be made executable for this to work
'''

CSV_ARGS = {'delimiter': ',',
            'quotechar': '"',
            'quoting': csv.QUOTE_MINIMAL,
            'dialect': 'excel'}

GET_HEADERS = {'accept': 'application/json'}
POST_HEADERS = {'accept': 'application/json', 'content-type': 'application/json'}


def get_args():
    import argparse
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('infile',
                        help='CSV file metadata to POST',
                        nargs='?',
                        type=argparse.FileType('rU'),
                        default=sys.stdin)
    parser.add_argument('--outfile',
                        help='CSV output report',
                        type=argparse.FileType(mode='wb', bufsize=0),
                        default=sys.stdout)
    parser.add_argument('--debug',
                        help="Print debug messages",
                        default=False, action='store_true')
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("./keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("./keypairs.json")))
    parser.add_argument('--update',
                        help="POST data to server, default is False.",
                        default=False, action='store_true')
    parser.add_argument('--encvaldata',
                        help="Directory in which https://github.com/ENCODE-DCC/encValData.git is cloned.\
                        Default is --encvaldata=%s" % (os.path.expanduser("./encValData/")),
                        default=os.path.expanduser("./encValData/"))
    parser.add_argument('--validatefiles',
                        help="validateFiles program needed to run script.  Default is --validatefiles=%s" % (os.path.expanduser("./validateFiles")),
                        default=os.path.expanduser("./validateFiles"))

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    else:  # use the defaulf logging level
        logging.basicConfig(format='%(levelname)s:%(message)s')

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if not os.path.isdir(args.encvaldata):
        logger.error('No ENCODE validation data.  git clone https://github.com/ENCODE-DCC/encValData.git')
        sys.exit(1)
    if not os.path.exists(args.validatefiles):
        logger.error("validateFiles not found. See http://hgdownload.cse.ucsc.edu/admin/exe/")
        sys.exit(1)

    return args


def md5(path):
    md5sum = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            md5sum.update(chunk)
    return md5sum.hexdigest()


def test_encode_keys(connection):
    test_URI = "ENCBS000AAA"
    url = urljoin(connection.server, test_URI)
    r = requests.get(url, auth=connection.auth, headers=connection.headers)
    try:
        r.raise_for_status()
    except:
        logger.debug('test_encode_keys got response %s' % (r.text))
        return False
    else:
        return True


def input_csv(fh):
    csv_args = CSV_ARGS
    return csv.DictReader(fh, **csv_args)


def output_csv(fh, fieldnames):
    csv_args = CSV_ARGS
    additional_fields = ['accession', 'aws_return']
    output_fieldnames = [fn for fn in fieldnames if fn] + additional_fields
    output = csv.DictWriter(fh, fieldnames=output_fieldnames, **csv_args)
    return output


def init_csvs(in_fh, out_fh):
    input_reader = input_csv(in_fh)
    flowcells = ["flowcell", "machine", "lane", "barcode"]
    headers = list(input_reader.fieldnames)
    details = False
    for f in flowcells:
        if f in headers:
            headers.remove(f)
            details = True
    if details:
        headers.append("flowcell_details")
    output_writer = output_csv(out_fh, headers)
    return input_reader, output_writer


def validate_file(f_obj, encValData, assembly=None, as_path=None):
    path = f_obj.get('submitted_file_name')
    file_format = f_obj.get('file_format')
    file_format_type = f_obj.get('file_format_type')
    output_type = f_obj.get('output_type')

    gzip_types = [
        "CEL",
        "bam",
        "bed",
        "csfasta",
        "csqual",
        "fasta",
        "fastq",
        "gff",
        "gtf",
        "tar",
        "sam",
        "wig"
    ]

    magic_number = open(path, 'rb').read(2)
    is_gzipped = magic_number == b'\x1f\x8b'
    if file_format in gzip_types:
        if not is_gzipped:
            logger.warning('%s: Expect %s format to be gzipped' % (path, file_format))
    else:
        if is_gzipped:
            logger.warning('%s: Expect %s format to be un-gzipped' % (path, file_format))

    if assembly:
        chromInfo = '-chromInfo=%s/%s/chrom.sizes' % (encValData, assembly)
    else:
        chromInfo = None

    if as_path:
        as_file = '-as=%s' % (as_path)
    else:
        as_file = None

    validate_map = {
        ('fasta', None): ['-type=fasta'],
        ('fastq', None): ['-type=fastq'],
        ('bam', None): ['-type=bam', chromInfo],
        ('bigWig', None): ['-type=bigWig', chromInfo],
        ('bed', 'bed3'): ['-type=bed3', chromInfo],
        ('bigBed', 'bed3'): ['-type=bed3', chromInfo],
        ('bed', 'bed3+'): ['-type=bed3+', chromInfo],
        ('bigBed', 'bed3+'): ['-type=bed3+', chromInfo],
        ('bed', 'bed6'): ['-type=bed6+', chromInfo],
        ('bigBed', 'bed6'): ['-type=bigBed6+', chromInfo],
        ('bed', 'bedLogR'): ['-type=bed9+1', chromInfo, '-as=%s/as/bedLogR.as' % encValData],
        ('bigBed', 'bedLogR'): ['-type=bigBed9+1', chromInfo, '-as=%s/as/bedLogR.as' % encValData],
        ('bed', 'bedMethyl'): ['-type=bed9+2', chromInfo, '-as=%s/as/bedMethyl.as' % encValData],
        ('bigBed', 'bedMethyl'): ['-type=bigBed9+2', chromInfo, '-as=%s/as/bedMethyl.as' % encValData],
        ('bed', 'broadPeak'): ['-type=bed6+3', chromInfo, '-as=%s/as/broadPeak.as' % encValData],
        ('bigBed', 'broadPeak'): ['-type=bigBed6+3', chromInfo, '-as=%s/as/broadPeak.as' % encValData],
        ('bed', 'gappedPeak'): ['-type=bed12+3', chromInfo, '-as=%s/as/gappedPeak.as' % encValData],
        ('bigBed', 'gappedPeak'): ['-type=bigBed12+3', chromInfo, '-as=%s/as/gappedPeak.as' % encValData],
        ('bed', 'narrowPeak'): ['-type=bed6+4', chromInfo, '-as=%s/as/narrowPeak.as' % encValData],
        ('bigBed', 'narrowPeak'): ['-type=bigBed6+4', chromInfo, '-as=%s/as/narrowPeak.as' % encValData],
        ('bed', 'bedRnaElements'): ['-type=bed6+3', chromInfo, '-as=%s/as/bedRnaElements.as' % encValData],
        ('bigBed', 'bedRnaElements'): ['-type=bed6+3', chromInfo, '-as=%s/as/bedRnaElements.as' % encValData],
        ('bed', 'bedExonScore'): ['-type=bed6+3', chromInfo, '-as=%s/as/bedExonScore.as' % encValData],
        ('bigBed', 'bedExonScore'): ['-type=bigBed6+3', chromInfo, '-as=%s/as/bedExonScore.as' % encValData],
        ('bed', 'bedRrbs'): ['-type=bed9+2', chromInfo, '-as=%s/as/bedRrbs.as' % encValData],
        ('bigBed', 'bedRrbs'): ['-type=bigBed9+2', chromInfo, '-as=%s/as/bedRrbs.as' % encValData],
        ('bed', 'enhancerAssay'): ['-type=bed9+1', chromInfo, '-as=%s/as/enhancerAssay.as' % encValData],
        ('bigBed', 'enhancerAssay'): ['-type=bigBed9+1', chromInfo, '-as=%s/as/enhancerAssay.as' % encValData],
        ('bed', 'modPepMap'): ['-type=bed9+7', chromInfo, '-as=%s/as/modPepMap.as' % encValData],
        ('bigBed', 'modPepMap'): ['-type=bigBed9+7', chromInfo, '-as=%s/as/modPepMap.as' % encValData],
        ('bed', 'pepMap'): ['-type=bed9+7', chromInfo, '-as=%s/as/pepMap.as' % encValData],
        ('bigBed', 'pepMap'): ['-type=bigBed9+7', chromInfo, '-as=%s/as/pepMap.as' % encValData],
        ('bed', 'openChromCombinedPeaks'): ['-type=bed9+12', chromInfo, '-as=%s/as/openChromCombinedPeaks.as' % encValData],
        ('bigBed', 'openChromCombinedPeaks'): ['-type=bigBed9+12', chromInfo, '-as=%s/as/openChromCombinedPeaks.as' % encValData],
        ('bed', 'peptideMapping'): ['-type=bed6+4', chromInfo, '-as=%s/as/peptideMapping.as' % encValData],
        ('bigBed', 'peptideMapping'): ['-type=bigBed6+4', chromInfo, '-as=%s/as/peptideMapping.as' % encValData],
        ('bed', 'shortFrags'): ['-type=bed6+21', chromInfo, '-as=%s/as/shortFrags.as' % encValData],
        ('bigBed', 'shortFrags'): ['-type=bigBed6+21', chromInfo, '-as=%s/as/shortFrags.as' % encValData],
        ('rcc', None): ['-type=rcc'],
        ('idat', None): ['-type=idat'],
        ('bedpe', None): ['-type=bed3+', chromInfo],
        ('bedpe', 'mango'): ['-type=bed3+', chromInfo],
        ('gtf', None): None,
        ('tar', None): None,
        ('tsv', None): None,
        ('csv', None): None,
        ('2bit', None): None,
        ('csfasta', None): ['-type=csfasta'],
        ('csqual', None): ['-type=csqual'],
        ('CEL', None): None,
        ('sam', None): None,
        ('wig', None): None,
        ('hdf5', None): None,
        ('gff', None): None
    }

    validate_args = validate_map.get((file_format, file_format_type), "")

    if validate_args == "":
        logger.warning('No rules to validate file_format %s and file_format_type %s' % (file_format, file_format_type))
        return False
    if validate_args is not None:
        if (file_format, file_format_type) in [('bed', 'bed3'), ('bed', 'bed3+')] and as_file:  # TODO: Update file schema and change to bed3+
            validate_args = ['-type=bed3+', chromInfo]  # TODO: Update file schema.  This is to foce bed3+ for validateFiles but pass bed3 to file_format_type
            validate_args.append(as_file)

        tokens = ['validateFiles'] + validate_args + [path]
        logger.debug('Running: %s' % (tokens))
        try:
            subprocess.check_output(tokens)
        except subprocess.CalledProcessError as e:
            logger.error("validateFiles returned %s" % (e.output))
            return False
        else:
            logger.debug("%s: validateFiles passed" % (path))
            return True
    else:
        return True


def post_file(file_metadata, connection, update=False):
    local_path = file_metadata.get('submitted_file_name')
    if not file_metadata.get('md5sum'):
        file_metadata['md5sum'] = md5(local_path)
    try:
        logger.debug("POST JSON: %s" % (json.dumps(file_metadata)))
    except:
        pass
    if update:
        url = urljoin(connection.server, '/files/')
        r = requests.post(url, auth=connection.auth, headers=connection.headers, data=json.dumps(file_metadata))
        try:
            r.raise_for_status()
        except:
            logger.warning('POST failed: %s %s' % (r.status_code, r.reason))
            logger.warning(r.text)
            return None
        else:
            return r.json()['@graph'][0]
    else:
        file_obj = copy.copy(file_metadata)
        file_obj.update({'accession': None})
        return file_obj


def upload_file(file_obj, update=False):
    if update:
        creds = file_obj['upload_credentials']
        logger.debug('AWS creds: %s' % (creds))
        env = os.environ.copy()
        env.update({
            'AWS_ACCESS_KEY_ID': creds['access_key'],
            'AWS_SECRET_ACCESS_KEY': creds['secret_key'],
            'AWS_SECURITY_TOKEN': creds['session_token'],
        })
        path = file_obj.get('submitted_file_name')
        try:
            subprocess.check_call(['aws', 's3', 'cp', path, creds['upload_url']], env=env)
        except subprocess.CalledProcessError as e:
            # The aws command returns a non-zero exit code on error.
            logger.error("AWS upload failed with exit code %d" % (e.returncode))
            return e.returncode
        else:
            return 0
    else:
        return None


def get_asfile(uri_json, connection):
    try:
        uris = json.loads(uri_json)
    except:
        logger.error("Could not parse as JSON: %s" % (uri_json))
        return None
    for uri in uris:
        url = urljoin(connection.server, uri)
        r = requests.get(url, headers=connection.headers, auth=connection.auth)
        try:
            r.raise_for_status()
        except:
            logger.error("Failed to get ENCODE object %s" % (uri))
            return None
        document_obj = r.json()
        r = requests.get(urljoin(connection.server, document_obj['uuid'] + '/' + document_obj['attachment']['href']), auth=connection.auth)
        try:
            r.raise_for_status()
        except:
            logger.error("Failed to download ENCODE document %s" % (uri))
            return None
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(r.text)
        return f


def process_row(row, connection):
    json_payload = {}
    flowcell_dict = {}
    if row.get("file_format", "") == "fastq":
        for header, sequence, qual_header, quality in encodedcc.fastq_read(connection, filename=row["submitted_file_name"]):
                sequence = sequence.decode("UTF-8")
                read_length = len(sequence)
                json_payload.update({"read_length": read_length})
    for key in row.keys():
        k = key.split(":")
        if k[0] in ["flowcell", "machine", "lane", "barcode"]:
            flowcell_dict[k[0]] = row[k[0]]
        else:
            if k[1] in ["int", "integer"]:
                value = int(row[k[0]])
            elif k[1] in ["list", "array"]:
                value = row[k[0]].strip("[]").split(",")
            if not k[0]:
                continue
            try:
                json_payload.update({k[0]: json.loads(value)})
            except:
                try:
                    json_payload.update({k[0]: json.loads('"%s"' % (value))})
                except:
                    logger.warning('Could not convert field %s value %s to JSON' % (k[0], value))
                    return None
    if any(flowcell_dict):
        flowcell_list = [flowcell_dict]
        json_payload.update({"flowcell_details": flowcell_list})
    if type(json_payload.get("paired_end")) == int:
        if json_payload["paired_end"] == 1:
            json_payload.pop("paired_with")
        json_payload["paired_end"] = str(json_payload["paired_end"])
    print(json_payload)
    return json_payload


def main():

    args = get_args()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    if not test_encode_keys(connection):
        logger.error("Invalid ENCODE server or keys: server=%s auth=%s" % (connection.server, connection.auth))
        sys.exit(1)

    input_csv, output_csv = init_csvs(args.infile, args.outfile)

    for n, row in enumerate(input_csv, start=2):  # row 1 is the header
        if row.get("file_format_specifications"):  #if there is no "file_format_spec" then no point in running get_asfile()
            as_file = get_asfile(row['file_format_specifications'], connection)
            as_file.close()  # validateFiles needs a closed file for -as, otherwise it gives a return code of -11
            validated = validate_file(row, args.encvaldata, row.get('assembly'), as_file.name)
            os.unlink(as_file.name)
        else:
            validated = validate_file(row, args.encvaldata, row.get('assembly'))

        if not validated:
            logger.warning('Skipping row %d: file %s failed validation' % (n, row['submitted_file_name']))
            continue

        json_payload = process_row(row, connection)
        if not json_payload:
            logger.warning('Skipping row %d: invalid field format for JSON' % (n))
            continue

        file_object = post_file(json_payload, connection, args.update)
        if not file_object:
            logger.warning('Skipping row %d: POST file object failed' % (n))
            continue

        aws_return_code = upload_file(file_object, args.update)
        if aws_return_code:
            logger.warning('Row %d: Non-zero AWS upload return code %d' % (aws_return_code))

        output_csv.writeheader()
        output_row = {}
        for key in output_csv.fieldnames:
            output_row.update({key: file_object.get(key)})
        output_row.update({'aws_return': aws_return_code})

        output_csv.writerow(output_row)


if __name__ == '__main__':
    main()
