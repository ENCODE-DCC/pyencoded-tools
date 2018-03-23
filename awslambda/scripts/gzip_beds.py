#! /usr/bin/python3

import boto3
import json
import csv
from botocore.config import Config

config = Config(
    retries=dict(
        max_attempts=10
    ),
    read_timeout=300
)

client = boto3.client('lambda', config=config)
BUCKET = 'regulomedb'
# FOLDER = 'Histone_Modification/REMC_core_127'

rocket = {
    "Records":
        [{
            "s3": {
                "bucket": {
                    "arn": "arn:aws:s3:::" + BUCKET,
                    "name": BUCKET
                },
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

out = open('reg_remc_bigbed.tsv', 'w')
with open('reg_remc_hmm_beds.tsv.2', 'r') as bed_list:
    # https://medium.com/@adds68/parsing-tsv-file-with-csv-in-python-662d6347b0cd
    reader = csv.DictReader(bed_list, dialect='excel-tab')

    for bed in reader:

        print("{}\t{}".format(bed['dataset'], bed['submitted_file_name']))
        payload = rocket
        payload['Records'][0]['s3']['object'] = { "key": bed['submitted_file_name']}
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
        results = json.loads(returns)

        if (results.get('errorMessage', "")):
            exit()

        bed['submitted_file_name'] = results['submitted_file_name']
        bed['md5sum'] = results['md5sum']
        out.write("\t".join(bed.values())+"\n")
