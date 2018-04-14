#! /usr/bin/python3

import boto3
import json
import os
import re
import urllib.parse as urlparse
from botocore.config import Config

config = Config(
    retries=dict(
        max_attempts=10
    ),
    read_timeout=300
)

client = boto3.client('lambda', config=config)
s3 = boto3.client('s3', config=config)

BUCKET = 'regulomedb'
FOLDERS = ['Motifs/Jolma', 'Motifs/PWMs', 'Motifs/UniPROBE', 'Motifs']
# have biosamples 'Motifs/WellingtonFootprints.FDR0.01'

rocket = {
    "Records":
        [{
            "s3": {
                "bucket": {
                    "arn": "arn:aws:s3:::" + BUCKET,
                    "name": BUCKET
                },
                "params": {}
            },
            "awsRegion": "us-west-2"
        }]
}
'''
Template for object[key]: property of s3.
    "object": {
        "key": "Histone_Modification/REMC_core_127/E001_15_coreMarks_mnemonics.bed"
    }
'''
def get_keys(bucket, prefix):
    """Get s3 objects from a bucket/prefix
    https://stackoverflow.com/questions/36789490/how-to-use-boto3-or-other-python-to-list-the-contents-of-a-requesterpays-s3
    """
    extra_kwargs = {}

    next_token = 'init'
    while next_token:
        kwargs = extra_kwargs.copy()
        if next_token != 'init':
            kwargs.update({'ContinuationToken': next_token})

        resp = s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix, **kwargs)

        try:
            next_token = resp['NextContinuationToken']
        except KeyError:
            next_token = None

        for contents in resp['Contents']:
            key = contents['Key']
            yield key


skip = [
]
out = open('reg_motif_allbio_files.tsv', 'a')
for f in FOLDERS:
    keys = list(get_keys(BUCKET, f))
    for fname in keys:
        #rint 'sub folder : ', o.get('Prefix')for fname in files:

        print("gzipping {}".format(fname))
        skip_it = False
        for skip_me in skip:
            if re.search(skip_me, fname):
                print("skipping {}".format(fname))
                skip_it = True
                break
        if skip_it:
            continue

        payload = rocket
        payload['Records'][0]['s3']['object'] = { "key": fname}
        # print(json.dumps(payload, sort_keys=True, indent=4))
        for retry in (1, 2, 3, 4, 5):
            try:
                res = client.invoke(FunctionName='reg_gzip_md5_split', InvocationType='RequestResponse', Payload=json.dumps(payload))
            except Exception:
                print("Retry: %s" % retry)
            finally:
                break

        returns = res['Payload'].read().decode()
        print("Status: {} Results {}".format(res['StatusCode'], returns))
        headers = ['file_size', 'target_label', 'submitted_file_name', 'md5sum']
        try:
            results = json.loads(returns)
            for new_file in results:

                urlp = urlparse.urlparse(new_file['submitted_file_name'])
                (folder, fn) = os.path.split(urlp.path)
                new_file['target_label'] = fn.split('.')[0]
                # use headers array to preserve order on re-run
                out.write("\t".join([str(new_file.get(x,"unknown")) for x in headers])+"\n")
        except Exception as e:
            print("Error formatting output: {}".format(e))


