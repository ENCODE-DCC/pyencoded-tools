
import urllib.parse
import boto3
import re
import hashlib

CHUNKSIZE = 4096
print('Loading function')

s3 = boto3.client('s3')

'''
def calculatemd5FromFile(filepath, chunksize=4096):
    hash_md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(chunksize), b''):
            hash_md5.update(chunk)
'''

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

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
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: response['Body'].read(amt=CHUNKSIZE), b''):
        hash_md5.update(chunk)

    return {'md5sum': hash_md5.hexdigest(), 'submitted_file_name': 's3://'+bucket+'/'+key}
