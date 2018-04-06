
import urllib.parse
import boto3
import re
import os
from gzip import GzipFile
from io import BytesIO
import hashlib
CHUNKSIZE = 4096

print('Loading function')

s3 = boto3.client('s3')


def split_on_column(fh, folder, col=3):

    targets = {}
    try:
        os.mkdir(folder)
    except FileExistsError:
        pass

    for peak in fh.readlines():
        fields = peak.split('\t')
        # chr start stop target something something something
        for t in fields[col].split('+'):
            t = re.sub('/', '::', t)
            fn = t + '.bed'
            outfh = open('/'.join([folder, fn]), 'a')
            outfh.write("\t".join(fields[0:col-1]+[t]+fields[col+1:]))
            targets[fn] = (folder, fn)

    return targets


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("Bucket %s Key %s" % (bucket, key))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    tag = re.sub(r'\W', '', response['ETag'])
    print("ETag: " + tag)
    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 9, gz_body)
    gz.write(response['Body'].read())
    gz.close()
    try:
        put_res = s3.put_object(Bucket=bucket, Key=key+'.gz', Body=gz_body.getvalue())
    except Exception as e:
        print(e)
        print('Error putting gzipped object {} to bucket {}.'.format(key+'.gz', bucket))
        print(put_res)
        raise e

    try:
        zip_response = s3.get_object(Bucket=bucket, Key=key+'.gz')
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: zip_response['Body'].read(amt=CHUNKSIZE), b''):
        hash_md5.update(chunk)

    return {'md5sum': hash_md5.hexdigest(), 'submitted_file_name': 's3://'+bucket+'/'+key+'.gz'}


def main():
    # for local testing ONLY
    import argparse
    parser = argparse.ArgumentParser(description="Parse input file")
    parser.add_argument('files', metavar='f', type=str, nargs='+',
                        help='list of files')

    args = parser.parse_args()
    for f in args.files:
        fh = open(f, 'r')
        froot = os.path.basename(f).split('.')[0]
        targs = split_on_column(fh, froot)
        fh.close()
        print(" ".join([froot] + [str(x) for x in targs.keys()]))


if __name__ == '__main__':
    main()