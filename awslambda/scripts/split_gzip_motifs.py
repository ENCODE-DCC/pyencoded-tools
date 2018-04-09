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
FOLDER = 'Motifs/Footprints'

rocket = {
    "Records":
        [{
            "s3": {
                "bucket": {
                    "arn": "arn:aws:s3:::" + BUCKET,
                    "name": BUCKET
                },
                "params": {
                    "split_column": 3
                }
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
skip = ['8988t_footprints']
out = open('reg_motif_BigFootprints_files.tsv', 'a')
list_result = s3.list_objects(Bucket=BUCKET, Prefix=FOLDER)
for fname in [x['Key'] for x in list_result.get('Contents')]:
    #rint 'sub folder : ', o.get('Prefix')for fname in files:

    skip_it = False
    for skip_me in skip:
        if re.search(skip_me, fname):
            print("skipping {}".format(fname))
            skip_it = True
            break
    if skip_it:
        continue

    print("splitting {}".format(fname))
    payload = rocket
    payload['Records'][0]['s3']['object'] = { "key": fname}
    # print(json.dumps(payload, sort_keys=True, indent=4))
    for retry in (1, 2, 3, 4, 5):
        try:
            res = client.invoke(FunctionName='reg_gzip_md5', InvocationType='RequestResponse', Payload=json.dumps(payload))
        except Exception:
            print("Retry: %s" % retry)
        finally:
            break

    returns = res['Payload'].read().decode()
    print("Status: {} Results {}".format(res['StatusCode'], returns))
    try:
        results = json.loads(returns)
        for new_file in results:

            urlp = urlparse.urlparse(new_file['submitted_file_name'])
            (folder, fn) = os.path.split(urlp.path)
            new_file['biosample_term_name'] = folder.split('/')[-1]
            new_file['target_label'] = fn.split('.')[0]

            out.write("\t".join([str(x) for x in new_file.values()])+"\n")
    except Exception as e:
        print("Error formatting output: {}".format(e))


