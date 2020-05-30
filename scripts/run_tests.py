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
from qancode import QANCODE


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("testurl", action='store', help="""URL of the RC or demo.""")
    parser.add_argument(
        "compare_url",
        action='store',
        default='https://www.encodeproject.org',
        help="""URL of standard to compare test url."""
    )
    parser.add_argument("--dry-run", action='store_true', help="""Just print some stuff.""")
    parser.add_argument(
        '-t',
        '--test-users',
        nargs='*',
        default=['2', '4'],
        help="""Which test users to run with."""
    )
    parser.add_argument(
        '-s',
        "--skip-public-user",
        action='store_true',
        default=False,
        help="""Do not run public user tests."""
    )
    parser.add_argument(
        '-b',
        "--browsers",
        nargs='*',
        default=['Chrome-headless', 'Firefox-headless'],
        help="""Which browsers to run with."""
    )
    parser.add_argument(
        '-p',
        "--processes",
        nargs='*',
        default=['compare_facets', 'check_trackhubs', 'find_differences'],
        help="""Which browsers to run with."""
    )
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
    if len(browsers) > 1:
        shuffled_browsers = [0, 1, random.randint(0, 1)]
        random.shuffle(shuffled_browsers)
        for user, browser_index in zip(users, shuffled_browsers):
            user_browser.append((user, browsers[browser_index]))
    else:
        for user in users:
            user_browser.append((user, browsers[0]))
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
    prod_url = args.compare_url

    clean_old_cookies()
    if os.path.exists(os.path.expanduser('~/output')):
        if os.path.exists(os.path.expanduser('~/output_archive')):
            shutil.rmtree(os.path.expanduser('~/output_archive'))
        shutil.copytree(os.path.expanduser('~/output'), os.path.expanduser('~/output_archive'))
        shutil.rmtree(os.path.expanduser('~/output'))
    os.mkdir(os.path.expanduser('~/output'))

    qa = QANCODE(rc_url=rc_url, prod_url=prod_url)
    browsers = args.browsers
    processes = args.processes
    users = [
        "encoded.test@gmail.com" if test_users_number == '1' else "encoded.test" + test_users_number + "@gmail.com"
        for test_users_number in args.test_users
    ]
    if not args.skip_public_user:
        users.insert(0, 'Public')
    print(f"Comparing {rc_url} to {prod_url}")
    print(f"for processes: {processes}")
    print(f"with browsers: {browsers}")
    print(f"and users: {users}\n")
    if not args.dry_run and qa and browsers and users:
        _run(qa, browsers, users, processes)


def _run(qa, browsers, users, processes):
    print('Start: {}'.format(datetime.datetime.now()))
    run_procs = []
    for proc in processes:
        func_name = f"run_{proc}"
        func = globals()[func_name]
        run_procs.append(
            multiprocessing.Process(target=func, args=(qa, browsers, users))
        )
    for run_proc in run_procs:
        run_proc.start()
    for run_proc in run_procs:
        run_proc.join()
    print('End: {}'.format(datetime.datetime.now()))
if __name__ == '__main__':
    main()
