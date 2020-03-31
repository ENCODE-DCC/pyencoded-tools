#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import json
import multiprocessing
import os
import random
import shutil
import sys

sys.path.insert(0, os.path.expanduser('~/pyencoded-tools'))
from qancode.qancode import QANCODE


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("testurl", action='store', help="""URL of the RC or demo.""")
    return parser


def clean_old_cookies():
    with open(os.path.expanduser('~/qa_credentials.json'), 'r') as f:
        info = json.load(f)
    for user in info:
        user['cookies'] = {}
    with open(os.path.expanduser('~/qa_credentials.json'), 'w') as f:
        json.dump(info, f)
    return


def generate_user_browser_tuples(browsers, users):
    user_browser = []
    for user in users:
        user_browser.append((user, browsers[random.randint(0, 1)]))
    return user_browser


def run_compare_facets(qa, browsers, users):
    compare_facets_settings = generate_user_browser_tuples(browsers, users)
    orig_stdout = sys.stdout
    for setting in compare_facets_settings:
        f = open(os.path.expanduser('~/output/compare_facets_{}_{}.txt'.format(setting[0], setting[1])), 'w')
        sys.stdout = f
        qa.compare_facets(
            users=[setting[0]],
            browsers=[setting[1]])
        f.close()
    sys.stdout = orig_stdout


def run_check_trackhubs(qa, browsers, users):
    check_trackhubs_settings = generate_user_browser_tuples(browsers, users)
    for setting in check_trackhubs_settings:
        qa.check_trackhubs(
            users=[setting[0]],
            browsers=[setting[1]],
            output_directory='~/output/check_trackhubs_{}_{}'.format(setting[0], setting[1]))


def run_find_differences(qa, browsers, users):
    find_diff_settings = generate_user_browser_tuples(browsers, users)
    for setting in find_diff_settings:
        qa.find_differences(
            users=[setting[0]],
            browsers=[setting[1]],
            output_directory='~/output/find_differences_{}_{}'.format(setting[0], setting[1]))


def main():
    parser = get_parser()
    args = parser.parse_args()
    rc_url = args.testurl

    clean_old_cookies()
    if os.path.exists(os.path.expanduser('~/output')):
        if os.path.exists(os.path.expanduser('~/output_archive')):
            shutil.rmtree(os.path.expanduser('~/output_archive'))
        shutil.copytree(os.path.expanduser('~/output'), os.path.expanduser('~/output_archive'))
        shutil.rmtree(os.path.expanduser('~/output'))
    os.mkdir(os.path.expanduser('~/output'))

    qa = QANCODE(rc_url=rc_url)
    browsers = ['Chrome-headless', 'Firefox-headless']
    users = [
        'Public',
        # 'encoded.test1@gmail.com',
        'encoded.test2@gmail.com',
        # 'encoded.test3@gmail.com',
        'encoded.test4@gmail.com'
    ]
    return qa, browsers, users


if __name__ == '__main__':
    qa, browsers, users = main()
    print('Start: {}'.format(datetime.datetime.now()))
    cf_process = multiprocessing.Process(target=run_compare_facets(qa, browsers, users))
    ct_process = multiprocessing.Process(target=run_check_trackhubs(qa, browsers, users))
    fd_process = multiprocessing.Process(target=run_find_differences(qa, browsers, users))
    cf_process.start()
    ct_process.start()
    fd_process.start()
    print('End: {}'.format(datetime.datetime.now()))
