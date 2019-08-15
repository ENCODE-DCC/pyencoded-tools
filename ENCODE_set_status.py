#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
For provided ENCODE object identifiers, change (patch) stauts of each object,
as well as related objects, to the specific status. This is for DCC admin only.

Overall, this script accept one or more ENCODE portal IDs through either
command line option `-r/--records` or a file and change their status to new
status specified by `-s/--status` option through ENCODE "set_status" API. For
example:

    %(prog)s -r TSTSR254470 -s released
    %(prog)s -i list_of_ids.txt -s archived

By default, the script will perform dry run via ENCODE "set_status" API. To
make real change, use `--update` option:

    %(prog)s -r 6c8b0cec-324a-491f-b35f-c6bbbf7c7842 -s revoked --update

There are three other options which can be accepted by ENCODE "set_status" API:
`--block-children`, `--force-audit` and `--force-transition`. Please refer to
the help message below for details.

You will need to setup authetication and target portal URL with -m, --keyfile
and/or environment variables. Please check the help message below for details.
By default, it will target test portal. If you want to test two targets on the
production portal:

    %(prog)s -r /targets/HNRPLL-human /targets/KIAA2018-human/ -s released -m prod --force-transition

Log will be printed to stdout as well as into a file under either current
working directory or a directory specified by `-d/--log-dir`.
"""

import argparse
import json
import logging
import os
import time
import sys

import requests


DCC_MODES = {
    "dev": "https://test.encodedcc.org",
    "prod": "https://www.encodeproject.org"
}
TIMEOUT = 60
REQUEST_HEADERS_JSON = {'content-type': 'application/json'}


class Connection():
    """Handles communication with the ENCODE Portal regarding status change.

    This a simplified version of encode_utils' Connection class.

    You will need to provide authentication and target portal to establish
    proper connection. In general, it will try to get info from either
    a `keyfile` or environment variables with a proper `mode_host_key`. Check
    `_setup_connection` method for details.

    The log file will be opened in append mode in the directory specified by
    `log_dir`. By default, it will be under current working directory. Its name
    follows pattern: set_status_<YearMonthDay>-<Hour>h<Minute>m<Second>s.log
    """

    def __init__(self, mode_host_key, keyfile=None, log_dir='.'):
        log_name = 'set_status_{}.log'.format(time.strftime('%Y%m%d-%Hh%Mm%Ss'))
        self._setup_log(
            os.path.join(os.path.abspath(log_dir), log_name)
        )
        self._setup_connection(key=mode_host_key, keyfile=keyfile)

        # Validate connection
        try:
            requests.get(
                self.dcc_url,
                auth=self.auth,
                timeout=2
            ).raise_for_status()
        except requests.exceptions.RequestException:
            self.debug_logger.error(
                'Authentication failed on {}. '
                'Please check your key and host setting'.format(self.dcc_url)
            )
            raise

    def _setup_log(self, path, add_stdout=True):
        """
        Log will be set at DEBUG level and will output to `path` in append
        mode, thus no overwriting. If `add_stdout`, it will also be printed to
        stdout.
        """
        self.debug_logger = logging.getLogger('set_status_debug')
        level = logging.DEBUG
        self.debug_logger.setLevel(level)
        f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')
        fh = logging.FileHandler(filename=path, mode="a")
        fh.setLevel(level)
        fh.setFormatter(f_formatter)
        self.debug_logger.addHandler(fh)
        print('\nLog file: {}\n'.format(path))
        if add_stdout:
            sh = logging.StreamHandler(stream=sys.stdout)
            sh.setLevel(level)
            sh.setFormatter(f_formatter)
            self.debug_logger.addHandler(sh)

    def _setup_connection(self, key, keyfile=None):
        """
        The connection to ENCODE portal is set up in the following precedence:

        1. Checkout `keyfile` if specified explicitely and get both URL and
        authetication from it based on the `key`.
        2. If a `keyfile` is not specified or setup connection from the
        provided `keyfile` failed, get authentication from environment
        variables, DCC_API_KEY and DCC_SECRET_KEY, and use `key` to get URL.
        For this strategy, `key` can be either a host/url or a string, which is
        either "dev" for test portal or "prod" for production portal.
        3. Finally, try to find a keyfile at "~/keypair.json". If found, get
        both URL and authetication from it based on the `key`.
        """
        # Get connection setting from provided `keyfile` argument first.
        if keyfile:
            path = os.path.abspath(str(keyfile))
            try:
                with open(path) as fp:
                    keypairs = json.load(fp)
                self.dcc_url = keypairs[key]['server']
                self.auth = (keypairs[key]['key'], keypairs[key]['secret'])
                self.debug_logger.info(
                    'Got connection setting from {}.'.format(path)
                )
                self.debug_logger.info('Host URL is: {}'.format(self.dcc_url))
                return
            except Exception:
                self.debug_logger.debug(
                    'Faile to setup connetion from Keypair file {}.'
                    'Will try environment variables'.format(path)
                )

        # Get connection setting from key (host) and environment variables.
        # This is easy for having one credential and variable hosts
        if os.getenv('DCC_API_KEY', None) and os.getenv('DCC_SECRET_KEY', None):
            self.auth = (os.getenv('DCC_API_KEY'), os.getenv('DCC_SECRET_KEY'))
            if key in DCC_MODES:
                self.dcc_url = DCC_MODES[key]
            elif str(key).startswith('http'):
                self.dcc_url = str(key)
            else:
                self.dcc_url = 'https://' + str(key).strip('/')
            self.debug_logger.info(
                'Got connection setting from environment variables: '
                'DCC_API_KEY and DCC_SECRET_KEY.'
            )
            self.debug_logger.info('Host URL is: {}'.format(self.dcc_url))
            return

        # Get connection setting from potential "keypairs.json" under home dir
        try:
            path = os.path.expanduser('~/keypairs.json')
            with open(path) as fp:
                keypairs = json.load(fp)
            self.dcc_url = keypairs[key]['server']
            self.auth = (keypairs[key]['key'], keypairs[key]['secret'])
            self.debug_logger.info(
                'Got connection setting from {}.'.format(path)
            )
            self.debug_logger.info('Host URL is: {}'.format(self.dcc_url))
            return
        except Exception:
            self.debug_logger.error(
                'Fail to setup connection with '
                'Key: {} and Keypair file:{}'.format(key, keyfile)
            )
            raise

    def touch_record(self, rec_id):
        try:
            url = '/'.join([self.dcc_url.rstrip('/'), rec_id.lstrip('/')])
            requests.get(url, auth=self.auth).raise_for_status()
        except requests.exceptions.RequestException:
            self.debug_logger.error(
                'Fail to get object {}. '
                'Please check your permission.'.format(rec_id)
            )
            raise
        return True

    def set_status(self, object_id, status, update=False, force_audit=False,
                   force_transition=False, block_children=False):
        """Change status of an object on the Portal.

        Args:
            object_id: `str`. A DCC identifier for the object on the Portal.
            status: `str`. A new status which the `object_id` will be.
            update: `bool`. `True` means to change status for real. `False`
                (default) means to only check and clarify what will be changed.
            force_audit: `bool`. `True` means to ignore audits on records when
                changing status. `False` (default) means to check audits before
                changing status and block changes if there are bad audits.
            force_transition: `bool`. `True` means to overide the transition
                table of ENCODE API and force the API to make whatever status
                changes specified by the `status` argument. `False` (default)
                means to respect ENCODE API status transition table and do not
                make abnormal status changes.
            block_children: `bool`. `True` means to only change status for the
                object specified by `object_id`. `False` (default) means to
                change status for `object_id` and its related objects.

        Returns:
            `list`: The JSON response from the PATCH operation. A list of
                objects with the corresponding status change happened (live
                run) or will happen (dry run).

        Raises:
            AssertionError: if status of any object got changed in a dry run.
            requests.exceptions.HTTPError: if the return status is not ok.
        """
        params = {
            'block_children': bool(block_children),
            'force_audit': bool(force_audit),
            'force_transition': bool(force_transition),
            'update': bool(update)
        }
        url = '/'.join([
            self.dcc_url.rstrip('/'),
            object_id.strip('/'),
            "@@set_status?"
        ])
        msg = (
            "<<<<<< SETTING {encode_id} status through URL {url} with this "
            "payload:\n\n{payload}\n\n"
        )
        payload = {'status': status}
        payload.update(params)
        self.debug_logger.debug(msg.format(
            encode_id=object_id,
            url=url,
            payload=json.dumps(payload, indent=4, sort_keys=True)
        ))
        response = requests.patch(
            url,
            auth=self.auth,
            timeout=TIMEOUT,
            headers=REQUEST_HEADERS_JSON,
            params=params,
            json={'status': status}
        )
        res_json = response.json()
        if response.ok:
            if update:
                self.debug_logger.info("Success.")
                self.debug_logger.debug(
                    json.dumps(res_json["changed"], indent=4, sort_keys=True)
                )
                return
            else:
                self.debug_logger.info("DRY RUN is enabled.")
                assert not res_json["changed"]
                self.debug_logger.debug(
                    json.dumps(res_json["considered"], indent=4, sort_keys=True)
                )
                return
        self.debug_logger.debug("Failed to SET STATUS through {}".format(url))
        self.debug_logger.debug("<<<<<< DCC PATCH RESPONSE: ")
        self.debug_logger.debug(json.dumps(res_json, indent=4, sort_keys=True))
        response.raise_for_status()


def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-r", "--records", nargs="+", help="""
                       One or more DCC record identifiers.""")
    group.add_argument("-i", "--infile", help="""
                       An input file containing one or more DCC record
                       identifiers, one per line. Empty lines and lines
                       starting with '#' are skipped. """)
    parser.add_argument("-s", "--status", required=True, help="""
                        The new status for records specified by -r/--records or
                        -i/--infile input file.""")
    parser.add_argument("-m", "--mode-host-key", default='dev', help="""
                        A predefined key, a host/url (requires environmenmt
                        variables DCC_API_KEY and DCC_SECRET_KEY) or a key in
                        keyfile. Please check the `--keyfile` option for more
                        details. Default is %(default)s.""")
    parser.add_argument("--keyfile", help="""
                        Path to a keypairs file with specific customizations of
                        authetication and target portal.
                        {}""".format(Connection._setup_connection.__doc__))
    parser.add_argument("--update", action="store_true", help="""
                        Turn off dry run and make real changes.""")
    parser.add_argument("--block-children", action="store_true", help="""
                        Only change status for objects explicitely specified by
                        -r/--records or -i/--infile input file and do not
                        consider related objects.""")
    parser.add_argument("--force-audit", action="store_true", help="""
                        Ignore audits on records when changing status.""")
    parser.add_argument("--force-transition", action="store_true", help="""
                        Overide the transition table of ENCODE API and force
                        the API to make whatever status changes defined by
                        -s/--status.""")
    parser.add_argument("-d", "--log-dir", default='.', help="""
                        A directory for the log file. Default is the current
                        working directory.""")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    rec_ids = args.records

    # Setup connection
    conn = Connection(
        args.mode_host_key,
        keyfile=args.keyfile,
        log_dir=args.log_dir
    )

    # Get IDs from input file if not from command line option
    if not rec_ids:
        with open(args.infile) as f:
            rec_ids = [
                line.strip() for line in f
                if line.strip() and (not line.startswith('#'))
            ]

    for rec_id in rec_ids:
        # Make sure the record is real and accessible
        conn.touch_record(rec_id)
        conn.set_status(
            object_id=rec_id,
            status=args.status,
            update=args.update,
            force_audit=args.force_audit,
            force_transition=args.force_transition,
            block_children=args.block_children
        )


__doc__ = __doc__.format(
    setup_connection=Connection._setup_connection.__doc__
)


if __name__ == "__main__":
    main()
