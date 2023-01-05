import requests
import urllib.parse
import sys
import time
from time import sleep
import json
import pprint
import subprocess
import os
import gzip
import shutil

AUTHID = "" 
AUTHPW = "" 
keypair = (AUTHID, AUTHPW)

GET_HEADERS = {'accept': 'application/json'}
POST_HEADERS = {'accept': 'application/json',
                'Content-Type': 'application/json'}

#SERVER = "https://www.encodeproject.org/"
SERVER = "https://test.encodedcc.org/"

DEBUG_ON = False

def encoded_get(url, keypair=None, frame='object', return_response=False):
    url_obj = urllib.parse.urlsplit(url)
    new_url_list = list(url_obj)
    query = urllib.parse.parse_qs(url_obj.query)
    if 'format' not in query:
        new_url_list[3] += "&format=json"
    if 'frame' not in query:
        new_url_list[3] += "&frame=%s" % (frame)
    if 'limit' not in query:
        new_url_list[3] += "&limit=all"
    if new_url_list[3].startswith('&'):
        new_url_list[3] = new_url_list[3].replace('&', '', 1)
    get_url = urllib.parse.urlunsplit(new_url_list)
    max_retries = 10
    max_sleep = 10
    while max_retries:
        try:
            if keypair:
                response = requests.get(get_url,
                                        auth=keypair,
                                        headers=GET_HEADERS)
            else:
                response = requests.get(get_url, headers=GET_HEADERS)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError) as e:
            print >> sys.stderr, e
            sleep(max_sleep - max_retries)
            max_retries -= 1
            continue
        else:
            if return_response:
                return response
            else:
                return response.json()

def fix_read_name(name):
    return name.split(" ")[0].split("/")[0]

def fix_file(broken_file, fixed_file):
    with open(fixed_file, "w") as f1:
        with open(broken_file, "r") as f:
            mone = 0 
            for l in f:
                mone +=1
                read_name = ""
                if mone == 1:
                    read_name = fix_read_name(l)[1:]
                    f1.write("@"+read_name+"\n")
                else:
                    f1.write(l)
                mone = mone %4


def post_file(json_payload, file_path, keypair):

    url = SERVER+ "/file"
    r = requests.post(url, auth=keypair, data=json.dumps(json_payload), headers=POST_HEADERS)

    try:
        r.raise_for_status()
    except:
        print('Submission failed: %s %s' % (r.status_code, r.reason))
        print(r.text)
        raise
    item = r.json()['@graph'][0]
    print(json.dumps(item, indent=4, sort_keys=True))


    ####################
    # POST file to S3

    creds = item['upload_credentials']
    env = os.environ.copy()
    env.update({
        'AWS_ACCESS_KEY_ID': creds['access_key'],
        'AWS_SECRET_ACCESS_KEY': creds['secret_key'],
        'AWS_SECURITY_TOKEN': creds['session_token'],
    })

    # ~10s/GB from Stanford - AWS Oregon
    # ~12-15s/GB from AWS Ireland - AWS Oregon
    print("Uploading file.")
    start = time.time()
    try:
        subprocess.check_call(['aws', 's3', 'cp', file_path, creds['upload_url']], env=env)
    except subprocess.CalledProcessError as e:
        # The aws command returns a non-zero exit code on error.
        print("Upload failed with exit code %d" % e.returncode)
        sys.exit(e.returncode)
    else:
        end = time.time()
        duration = end - start
        print("Uploaded in %.2f seconds" % duration)

with open("file_to_fix", "r") as f:
    for l in f:
        acc = l.split("/")[-1].split(".")[0]
        url = SERVER + "/files/" + acc + "/"
        file = encoded_get(url, keypair)
        
        local_path = "/s3/" + file['s3_uri'][5:]
        print (local_path)
        ed = encoded_get(url, keypair, frame="edit")
        ed['derived_from'] = [ed['accession']]
        ed.pop('accession', None)
        ed['aliases'] = ["encode:new_"+acc]
        ed.pop('schema_version', None)
        ed.pop('date_created', None)
        ed.pop('read_count', None)
        ed.pop('submitted_file_name', None)
        ed.pop('content_md5sum', None)
        ed.pop('md5sum', None)
        ed.pop('controlled_by', None)
        ed.pop('dbxrefs', None)
        ed.pop('fastq_signature', None)
        ed.pop('alternate_accessions', None)
        ed.pop('status', None)
        ed.pop('submitted_by', None)
        ed.pop('no_file_available', None)
        ed.pop('flowcell_details', None)
        if ed['paired_end'] == "1" and 'paired_with' in ed:
            ed.pop('paired_with', None)
        elif ed['paired_end'] == "2" and 'paired_with' in ed:
            acc2 = ed['paired_with'].split("/")[2]
            ed['paired_with'] = "encode:new_" + acc2
        data_stream = subprocess.Popen(
            ['gunzip --stdout {}'.format(local_path)],
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE)

        with open(acc + '_fixed.fastq', "w") as fixed:
            mone = 0
            for encoded_line in data_stream.stdout:
                mone +=1
                try:
                    line = encoded_line.decode('utf-8')
                except UnicodeDecodeError:
                    print ('Error occured, while decoding the readname string.')
                else:
                    if mone == 1:
                        fixed.write("@" + fix_read_name(line)[1:] + "\n")
                    else:
                        fixed.write(line)
                mone = mone % 4

        print ('starting to gzip')            
        with open(acc + '_fixed.fastq', 'rb') as f_in, open(acc + '_fixed.fastq.gz', 'wb') as f_out:
            with gzip.GzipFile(fileobj=f_out, mode='wb', filename="", mtime=None) as gz_out:
                shutil.copyfileobj(f_in, gz_out)

        print ('calculating md5sum')
        output = subprocess.check_output(['md5sum', acc + '_fixed.fastq.gz'], stderr=subprocess.STDOUT)
                

        md5sum = output[:32].decode(errors='replace')
        print (md5sum)
        ed['md5sum'] = md5sum
        ed['step_run'] = "encode:fast-read-name-correction-step-run"
        
        post_file(ed, acc + '_fixed.fastq.gz', keypair)
        print ("finished to post")
        os.remove(acc + '_fixed.fastq')
        os.remove(acc + '_fixed.fastq.gz')
