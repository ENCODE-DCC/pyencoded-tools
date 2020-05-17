
import urllib.parse
import boto3
import re
import os
from gzip import GzipFile
from io import BytesIO
import hashlib
import glob
CHUNKSIZE = 4096

print('Loading function')

s3 = boto3.client('s3')

if __name__ == '__main__':
    PREFIX = ''
else:
    PREFIX = '/tmp/'



def split_on_column(fh, folder, col=3):

    peaks = {}
    try:
        os.mkdir(PREFIX+folder)
    except OSError:
        try:
            os.remove(glob.glob('/tmp/*'))
        except:
            pass
    except FileExistsError:
        pass

    try:
        flines = fh.readlines()
    except AttributeError:
        flines = fh.read().decode('utf8').split('\n')

    for peak in flines:
        fields = peak.split('\t')
        # chr start stop target something something something
        if len(fields) < col+1:
            break
        for t in fields[col].split('+'):
            t = re.sub('/', '::', t)
            peaks[t] = peaks.get(t,[])
            peaks[t].append("\t".join(fields[0:col]+[t]+fields[col+1:])+"\n")

    return peaks

def gzip_md5_fh(fh, bucket, key):

    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 9, gz_body)
    try:
        fh.read
        gz.write(fh.read())
    except AttributeError:
        # it's a list of strings
        for l in fh:
            gz.write(l.encode('utf8'))

    gz.close()
    try:
        put_res = s3.put_object(Bucket=bucket, Key=key+'.gz', Body=gz_body.getvalue())
    except Exception as e:
        print(e)
        print('Error putting gzipped object {} to bucket {}.'.format(key+'.gz', bucket))
        print(put_res)
        raise e

    if re.search('/tmp', key):
        os.remove(key)
        # keep /tmp clean
    try:
        zip_response = s3.get_object(Bucket=bucket, Key=key+'.gz')
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    hash_md5 = hashlib.md5()
    size = 0
    for chunk in iter(lambda: zip_response['Body'].read(amt=CHUNKSIZE), b''):
        size += chunk.__sizeof__()
        hash_md5.update(chunk)

    return {'md5sum': hash_md5.hexdigest(), 'submitted_file_name': 's3://'+bucket+'/'+key+'.gz', 'file_size': size}


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("Bucket %s Key %s" % (bucket, key))
    if re.search('.gz', key):
        return [{ "errorMessage": key+" already gzipped, probably"}]
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    split_column = event['Records'][0]['s3']['params'].get('split_column', None)
    print("Going to split file on column {}".format(split_column))
    outs = []
    if split_column:
        new_folder = os.path.basename(key).split('.')
        old_folder = os.path.dirname(key)
        new_folder = new_folder[0] + '_PWM_split'
        print("{} :: {}".format(old_folder, new_folder))
        peaks = split_on_column(response['Body'], new_folder, col=split_column)
        for target in peaks.keys():
            outs.append(gzip_md5_fh(peaks[target], bucket, "/".join([old_folder, new_folder, target+".bed"])))

    else:
        outs.append(gzip_md5_fh(response['Body'], bucket, key))

    return outs


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
