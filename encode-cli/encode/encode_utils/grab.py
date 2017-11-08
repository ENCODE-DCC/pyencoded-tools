import asyncio
import aiohttp
import json
import operator
import os
import numpy as np
import pandas as pd
import requests

from functools import wraps
from itertools import chain
from urllib.parse import urljoin

# Copied from pyencoded-tools/encodedcc.py to avoid dependency.


class ENC_Key:
    def __init__(self, keyfile, keyname):
        if os.path.isfile(str(keyfile)):
            keys_f = open(keyfile, 'r')
            keys_json_string = keys_f.read()
            keys_f.close()
            keys = json.loads(keys_json_string)
        else:
            keys = keyfile
        key_dict = keys[keyname]
        self.authid = key_dict['key']
        self.authpw = key_dict['secret']
        self.server = key_dict['server']
        if not self.server.endswith("/"):
            self.server += "/"


class ENC_Connection(object):
    def __init__(self, key):
        self.headers = {'content-type': 'application/json',
                        'accept': 'application/json'}
        self.server = key.server
        self.auth = (key.authid, key.authpw)


# Define key if private data desired.
key = ENC_Key(os.path.expanduser("~/keypairs.json"), 'prod')
auth = (key.authid, key.authpw)
base_url = 'https://www.encodeproject.org'
associated_search = urljoin(base_url, '/search/?type={}&{}={}&{}')
json_all = 'limit=all&format=json'
json_only = 'format=json'
request_auth = aiohttp.BasicAuth(key.authid, key.authpw)
loop = asyncio.get_event_loop()


def create_session():
    connector = aiohttp.TCPConnector(keepalive_timeout=100, limit=100)
    return aiohttp.ClientSession(connector=connector)


# Utils.


def make_associated_url(base_url):
    return urljoin(base_url, '/search/?type={}&{}={}&{}&frame=embedded')


def get_data(url):
    r = requests.get(url, auth=auth)
    try:
        assert r.status_code == 200
    except AssertionError as e:
        raise Exception(url, r.text) from e
    try:
        return r.json()['@graph']
    except KeyError:
        return r.json()


async def async_get_data(url, session, request_auth=request_auth):
    r = await session.get(url, auth=request_auth)
    try:
        assert r.status == 200
    except AssertionError as e:
        raise Exception(url, await r.text()) from e
    return await r.json()


def quick_grab_data(urls, session=None, loop=loop):
    f = [async_get_data(url, session) for url in urls]
    results = loop.run_until_complete(asyncio.gather(*f))
    try:
        return [subobject for item in results for subobject in item['@graph']]
    except KeyError:
        return results


def get_associated(item_type, related_field, related_ids, session=None):
    urls = [associated_search.format(item_type,
                                     related_field,
                                     related_id,
                                     json_all)
            for related_id in related_ids]
    return quick_grab_data(urls, session)
