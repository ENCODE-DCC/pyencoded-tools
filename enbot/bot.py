import curio
import os
import random
import requests
import subprocess
import time
import urllib.parse
import boto3


from datetime import datetime
from slackclient import SlackClient


BOT_NAME = 'enbot'
BOT_ID = 'U5TE2SW2V'
AT_BOT = '<@{}>'.format(BOT_ID)
# Must export SLACK_BOT_TOKEN='secret_api_key' in .bash_profile
SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
MONITORING_URLS = []

slack_client = SlackClient(SLACK_BOT_TOKEN)

servers_dict = {
    'Production': 'https://www.encodeproject.org/_indexer',
    'Test': 'https://test.encodedcc.org/_indexer'
}

image_list = ['https://upload.wikimedia.org/wikipedia/'
              'commons/4/45/A_small_cup_of_coffee.JPG',
              'http://www.speedylife.fr/photo/art/default/'
              '6716610-10265899.jpg?v=1402495406']


users_dict = {}
api_call = slack_client.api_call("users.list")
if api_call.get('ok'):
    users = api_call.get('members')
    for user in users:
        users_dict[user.get('id')] = user['name']


def get_status(server):
    url = servers_dict[server]
    r = requests.get(url)
    status = r.json()['status']
    response = "{} Status: {}".format(server,
                                      status.upper())
    return response


def parse_get_request(command):
    try:
        for_split = command.split(' for ')
        object_id = for_split[1].strip().upper()
        get_split = for_split[0].split('get ')
        field = get_split[1].strip().lower()
    except IndexError:
        return (None, None)
    return (object_id, field)


def get_field(object_id, field):
    url = 'https://encodeproject.org/{}/?format=json'
    url = url.format(object_id)
    r = requests.get(url)
    if r.status_code == 404:
        return 'object_not_found'
    else:
        r = r.json()
        if field == 'keys':
            return r.keys()
        else:
            return r.get(field, 'Field not found.')


async def poll_indexer(url, channel, instance_id=None):
    waiting_count = 0
    failed_get = 0
    start = time.time()
    while True:
        await curio.sleep(6)
        try:
            r = requests.get(url)
            r = r.json()
            status = r['status']
            failed_get = 0
            if status == 'waiting' and waiting_count < 20:
                waiting_count += 1
                continue
            elif status == 'indexing':
                waiting_count = 0
                start = time.time()
                continue
        except Exception as e:
            failed_get += 1
            if failed_get > 9:
                send_response('GET failure for {}. Aborting at {}.'.format(
                    url, datetime.now().strftime('%Y-%m-%d %H:%M:%S')), channel)
                break
            else:
                continue
        end = time.time()
        send_response(
            'DONE: Indexer {} status waiting for {} seconds at {}.'.format(
                url,
                int(end - start),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            channel
        )
        MONITORING_URLS.remove((url, channel))
        if instance_id:
            stop_instance(instance_id.id, channel)
        break


def parse_howdoi_request(command):
    try:
        howdoi_split = command.split('howdoi')
        query = howdoi_split[1].strip()
    except IndexError:
        return None
    return query


# def parse_image_diff_request(command):
#     try:
#         url_extract = command.split(' diff ')
#         url_split = url_extract[1].split(' and ')
#         query = [u.replace('<', '').replace('>', '').strip()
#                  for u in url_split]
#     except IndexError:
#         return None
#     return query
def send_response(response, channel, attachments=None):
    slack_client.api_call("chat.postMessage",
                          channel=channel,
                          text=response,
                          as_user=True,
                          attachments=attachments)


def find_instance_from_url(url):
    tag_name = url.split('/')[2].split('.demo')[0]
    print(tag_name)
    ec2 = boto3.resource('ec2', region_name='us-west-2')
    instances = (
        s
        for s in ec2.instances.filter(
                Filters=[{'Name': 'tag:Name', 'Values': [tag_name]}]
        )
    )
    if instances:
        return next(instances)
    else:
        return None


def stop_instance(instance_id, channel):
    print('stopping instance', instance_id)
    ec2 = boto3.resource('ec2', region_name='us-west-2')
    try:
        res = ec2.instances.filter(InstanceIds=[instance_id]).stop()
        assert res[0]['ResponseMetadata']['HTTPStatusCode'] == 200
        send_response('Demo instance {} stopped.'.format(instance_id), channel)
    except Exception as e:
        print(e)
        send_response('Failed to stop {}'.format(instance_id), channel)


async def handle_command(command, channel, timestamp):
    attachments = None
    command = command.lower()
    if 'coffee' in command:
        response = ""
        image_url = random.choice(image_list)
        attachments = [{"title": "",
                        "image_url": image_url}]
    # elif 'diff' in command:
    #     query = parse_image_diff_request(command)
    #     if query:
    #         text = 'Checking {} and {}.'.format(query[0], query[1])
    #         slack_client.api_call("chat.postMessage",
    #                               channel=channel,
    #                               text=text,
    #                               as_user=True,
    #                               attachments=attachments)
    #         try:
    #             qa = QANCODE(prod_url=query[0], rc_url=query[1])
    #             results = qa.find_differences(browsers=['Firefox'],
    #                                           users=['Public'],
    #                                           item_types=['/'])
    #             if results[0][0]:
    #                 directory = os.path.join(os.path.expanduser('~'),
    #                                          'Desktop',
    #                                          'image_diff')
    #                 full_path = os.path.join(directory, results[0][1])
    #                 with open(full_path, 'rb') as f:
    #                     slack_client.api_call("files.upload",
    #                                           channels=channel,
    #                                           title="Difference found.",
    #                                           file=f,
    #                                           filename=results[0][1],
    #                                           as_user=True)
    #                 response = None
    #             else:
    #                 response = 'Match'
    #         except:
    #             response = 'Image diff error. Exiting.'
    elif 'monitor' in command:
        global MONITORING_URLS
        response = None
        try:
            # This should probably be REGEX capture group.
            url = command.split(' ')[1].replace(
                '>', '').replace('<', '').strip()
            print(url)
            # Some Slack magic for URLs missing https://.
            if '|' in url:
                url = url.split('|')[1]
                url = 'https://{}'.format(url)
            print(url)
            url = urllib.parse.urljoin(url, '/_indexer')
            print(url)
            send_response('Checking URL.', channel)
            r = requests.get(url)
            if r.status_code != 200:
                response = 'Bad URL: {}'.format(url)
            else:
                if (url, channel) not in MONITORING_URLS:
                    MONITORING_URLS.append((url, channel))
                    send_response('START: Monitoring {} at {}.'.format(
                        url, datetime.now().strftime('%Y-%m-%d %H:%M:%S')), channel)
                    instance_id = None
                    if '.demo.encodedcc.org' in url:
                        instance_id = find_instance_from_url(url)
                        if instance_id:
                            send_response(
                                'Found demo instance {}. Will stop when indexing complete.'.format(instance_id.id),
                                channel
                            )
                    await curio.spawn(poll_indexer, url, channel, instance_id)
                else:
                    response = 'Already monitoring.'
        except Exception as e:
            print(e)
            if command.strip().endswith('monitor'):
                send_response(
                    'Currently monitoring: {}'.format('None' if len(MONITORING_URLS) == 0
                                                      else [m[0] for m in MONITORING_URLS
                                                            if m[1] == channel]), channel)
            else:
                response = 'Bad input.'
                print(e)
    elif 'howdoi' in command:
        query = parse_howdoi_request(command)
        if query:
            value = subprocess.check_output(['howdoi', query])
            response = '{}'.format(value.decode('utf-8'))
            if len(response) > 100:
                slack_client.api_call("files.upload",
                                      channels=channel,
                                      title="answer",
                                      content=response,
                                      as_user=True)
                response = None
    elif 'get' in command:
        object_id, field = parse_get_request(command)
        if object_id and field:
            value = get_field(object_id, field)
            if value == 'object_not_found':
                response = 'Object not found.'
            elif value:
                response = "*{}*\n_{}_: {}".format(object_id,
                                                   field,
                                                   value)
            else:
                response = 'Field is empty.'
        else:
            response = 'Invalid GET command.'
    elif 'prod' in command:
        server = 'Production'
        response = get_status(server)
    elif 'test' in command:
        server = 'Test'
        response = get_status(server)
    elif (('index' in command)
          or ('status' in command)):
        response_list = []
        for server in servers_dict.keys():
            response = get_status(server)
            response_list.append(response)
        response = "\n".join(response_list)
    else:
        response = ':hugging_face:'

    if response is not None:
        slack_client.api_call("chat.postMessage",
                              channel=channel,
                              text=response,
                              as_user=True,
                              attachments=attachments)
#   slack_client.api_call("reactions.add",
#                         channel=channel,
#                         name="thumbsup",
#                         timestamp=timestamp)


async def parse_slack_output(slack_rtm_output):
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                text = output['text'].split(AT_BOT)[1].strip().lower()
                channel = output['channel']
                timestamp = output['ts']
                print(users_dict[output['user']], output['text'])
                return (text, channel, timestamp)
    return (None, None, None)


async def main_loop():
    READ_WEBSOCKET_DELAY = 1
    retry = 10
    if slack_client.rtm_connect():
        print("ENBOT running.")
        while True:
            try:
                command, channel, timestamp = await parse_slack_output(
                    slack_client.rtm_read())
                if command and channel:
                    await handle_command(command, channel, timestamp)
                await curio.sleep(READ_WEBSOCKET_DELAY)
            except (KeyboardInterrupt, SystemExit):
                break
            except Exception as e:
                print(e)
                print(slack_client.rtm_connect())
                retry -= 1
                if retry < 0:
                    raise e
    else:
        print("Connection failed.")


if __name__ == '__main__':
    curio.run(main_loop)
