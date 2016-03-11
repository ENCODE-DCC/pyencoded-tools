import argparse
import os.path
import encodedcc
import requests
from urllib.parse import quote
import subprocess
import mimetypes
from PIL import Image
from base64 import b64encode
import magic  # install me with 'pip install python-magic'
# https://github.com/ahupp/python-magic
# this is the site for python-magic in case we need it

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--object',
                        help="Either the file containing a list of ENCs as a column,\
                        a single accession by itself, or a comma separated list of identifiers")
    parser.add_argument('--query',
                        help="query of objects you want to process")
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


def attachment(path):
    """ Create an attachment upload object from a filename
    Embeds the attachment as a data url.
    """

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


def file_manager(key, value, connection, obj_type):
    filename = key.split("/")[-1]
    print("Downloading {}".format(filename))
    r = requests.get(key)
    with open(filename, "wb") as outfile:
        outfile.write(r.content)
    if obj_type == "Biosample":
        filepart = filename.split("-")[0]
    else:
        filepart = filename.split("-")[1]

    attach = attachment(filename)
    temp = "_".join(key.split("/")[-2:])
    aliases = ["brenton-graveley:" + temp]
    upload = {"aliases": aliases,
              "attachment": attach,
              "award": "U54HG007005",
              "document_type": "general protocol",
              "lab": "/labs/brenton-graveley/",
              "status": "released",
              "description": "{obj_type} protocol for {filepart} shRNA followed by RNA-seq".format(obj_type=obj_type, filepart=filepart),
              }

    print("Uploading {} as {}".format(filename, aliases[0]))
    encodedcc.new_ENCODE(connection, "Document", upload)

    print("Patching {} with document {}".format(value, aliases[0]))
    if obj_type == "Biosample":
        docs = {"protocol_documents": aliases}
    else:
        docs = {"documents": aliases}
    encodedcc.patch_ENCODE(quote(value), connection, docs)

    print("Removing document {}".format(filename))
    subprocess.run(["rm", filename])


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    biosamples = [line.strip() for line in open("LV08_biosample_protocol_oneCellLine.txt")]
    libraries = [line.strip() for line in open("LV08_library_protocol.txt")]

    bio_alias = dict.fromkeys(biosamples)
    # http://graveleylab.cam.uchc.edu/ENCODE/ENCODE_DATA/protocol/LV08_biosample_protocol/DDX3X-LV08-15.pdf
    for key in bio_alias.keys():
        bio_alias[key] = "brenton-graveley:" + key.split("/")[-1].split(".")[0]

    lib_alias = dict.fromkeys(libraries)
    # http://graveleylab.cam.uchc.edu/ENCODE/ENCODE_DATA/protocol/LV08_library_protocol/L-AKAP1-LV08-3.pdf
    for key in lib_alias.keys():
        lib_alias[key] = "brenton-graveley:" + key.split("/")[-1].split(".")[0]

    for key in bio_alias.keys():
        file_manager(key, bio_alias[key], connection, "Biosample")
    for key in lib_alias.keys():
        file_manager(key, lib_alias[key], connection, "Library")


if __name__ == '__main__':
        main()
