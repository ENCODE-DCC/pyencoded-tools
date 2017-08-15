import json
import tempfile
import numpy as np
import os
import requests
import subprocess
import sys
import urllib

from .clickpaths import *
from .comparisons import *
from .defaults import USERS, BROWSERS, bcolors, ActionTuples
from .tasks import *
from .worker import DataWorker, DataManager

################################################################
# QANCODE object gets data, compares data using defined tasks. #
################################################################


class QANCODE(ActionTuples):
    """
    Object to keep track of Task/Comparison combinations and run QA
    process with one method call.
    """

    def __init__(self, rc_url, prod_url='https://www.encodeproject.org'):
        self.rc_url = rc_url
        self.prod_url = prod_url
        self.browsers = [b for b in BROWSERS]
        self.users = [u for u in USERS]
        # Default action attributes inherited from ActionTuples.
        super().__init__()

    def __repr__(self):
        return 'QANCODE(prod_url={}, rc_url={})'.format(self.prod_url,
                                                        self.rc_url)

    def list_methods(self):
        """
        List all possible tests.
        """
        print(*[f for f in sorted(dir(QANCODE)) if callable(getattr(QANCODE, f))
                and not f.startswith('__') and not f.startswith('_')], sep='\n')

    def compare_facets(self,
                       browsers='all',
                       users='all',
                       item_types='all',
                       task=GetFacetNumbers,
                       url_comparison=True,
                       browser_comparison=True):
        """
        Gets RC URL facet numbers and compares them to production URL facet
        numbers for given item_type, browser, user.
        """
        # Define item type pages (search and matrix views) to check.
        all_item_types = self.compare_facets_default_actions
        if browsers == 'all':
            browsers = self.browsers
        if users == 'all':
            users = self.users
        if item_types == 'all':
            item_types = [t for t in all_item_types]
        urls = [self.prod_url, self.rc_url]
        click_paths = [None for c in item_types]
        dm = DataManager(browsers=browsers,
                         urls=urls,
                         users=users,
                         item_types=item_types,
                         click_paths=click_paths,
                         task=task)
        dm.run_tasks()
        if url_comparison:
            for browser in browsers:
                for user in users:
                    for item_type in item_types:
                        cfn_url = CompareFacetNumbersBetweenURLS(browser=browser,
                                                                 user=user,
                                                                 prod_url=self.prod_url,
                                                                 rc_url=self.rc_url,
                                                                 item_type=item_type,
                                                                 click_path=None,
                                                                 all_data=dm.all_data)
                        cfn_url.compare_data()
        if browser_comparison:
            for url in urls:
                for user in users:
                    for item_type in item_types:
                        cfn_browser = CompareFacetNumbersBetweenBrowsers(user=user,
                                                                         url=url,
                                                                         item_type=item_type,
                                                                         browsers=browsers,
                                                                         all_data=dm.all_data)
                        cfn_browser.compare_data()

    @staticmethod
    def _expand_action_list(actions):
        """
        Returns item_types and click_paths given list of tuples.
        """
        item_types = [t[0] for t in actions]
        click_paths = [p[1] for p in actions]
        return item_types, click_paths

    def _parse_arguments(self,
                         browsers,
                         users,
                         item_types,
                         click_paths,
                         action_tuples,
                         actions,
                         admin_only_actions,
                         public_only_actions):
        if browsers == 'all':
            browsers = self.browsers
        if users == 'all':
            users = self.users
        if action_tuples is not None:
            item_types, click_paths = self._expand_action_list(action_tuples)
        else:
            if item_types == 'all':
                item_types, click_paths = self._expand_action_list(actions)
            elif item_types == 'random':
                # Choose random item_types from action list to check.
                # Note np.random.choice won't work for tuples.
                random_actions = [actions[i]
                                  for i in np.random.randint(len(actions) - 1, size=3)]
                item_types, click_paths = self._expand_action_list(
                    random_actions)
            elif item_types == 'admin':
                item_types, click_paths = self._expand_action_list(
                    admin_only_actions)
            elif item_types == 'public':
                item_types, click_paths = self._expand_action_list(
                    public_only_actions)
            else:
                if len(item_types) == len(click_paths):
                    pass
                elif len(click_paths) == 1:
                    # Broadcast single click_path (e.g. None) to all user-defined item_types.
                    click_paths = [click_paths[0] for t in item_types]
                else:
                    raise ValueError(
                        'item_types and click_paths must have same length')
        return browsers, users, item_types, click_paths

    def find_differences(self,
                         browsers='all',
                         users='all',
                         action_tuples=None,
                         item_types='all',
                         click_paths=[None],
                         task=GetScreenShot):
        """
        Does image diff for given item_types. If click_path defined will
        perform that action before taking screenshot.
        """
        # Tuple of (item_type, click_path)
        actions = self.find_differences_default_actions
        admin_only_actions = [('/biosamples/ENCBS681LAC/', None),
                              ('/search/?searchTerm=ENCBS681LAC&type=Biosample', None)]
        public_only_actions = [('/experiments/?status=deleted', None)]
        browsers, users, item_types, click_paths = self._parse_arguments(browsers=browsers,
                                                                         users=users,
                                                                         item_types=item_types,
                                                                         click_paths=click_paths,
                                                                         action_tuples=action_tuples,
                                                                         actions=actions,
                                                                         admin_only_actions=admin_only_actions,
                                                                         public_only_actions=public_only_actions)
        urls = [self.prod_url, self.rc_url]
        results = []
        with tempfile.TemporaryDirectory() as td:
            dm = DataManager(browsers=browsers,
                             urls=urls,
                             users=users,
                             item_types=item_types,
                             click_paths=click_paths,
                             task=task,
                             temp_dir=td)
            dm.run_tasks()
            for browser in browsers:
                for user in users:
                    for item_type, click_path in zip(item_types, click_paths):
                        css = CompareScreenShots(browser=browser,
                                                 user=user,
                                                 prod_url=self.prod_url,
                                                 rc_url=self.rc_url,
                                                 item_type=item_type,
                                                 click_path=click_path,
                                                 all_data=dm.all_data)
                        result = css.compare_data()
                        results.append(result)
        self.find_differences_output = results
        return self.find_differences_output

    def show_differences(self):
        """
        This is a Jupyter notebook helper function that loads image diffs
        into the notebook and displays them. Won't work in terminal.
        """
        try:
            # In Jupyter notebook?
            get_ipython().config
        except NameError:
            print('Must use in Jupyter notebook')
            return
        from IPython.core.display import display, HTML, Image
        try:
            for out in self.find_differences_output:
                if out[0]:
                    print('{}:'.format(out[1]))
                    display(
                        Image(filename='../../../image_diff/{}'.format(out[1])))
                    print('\n')
        except AttributeError:
            print('No image diffs found')

    def check_trackhubs(self, browsers=['Safari'], users=['Public'], action_tuples=None):
        """
        Runs find_differences() image diff for selected list of trackhub
        actions.
        """
        print('Running check trackhubs')
        trackhub_actions = self.check_trackhubs_default_actions
        if action_tuples is None:
            action_tuples = trackhub_actions
        self.find_differences(users=users, browsers=browsers,
                              action_tuples=action_tuples)

    def check_permissions(self, browsers=['Safari'], users=['Public']):
        """
        Runs find_differences() image diff on permission check pages.
        """
        print('Running check permissions')
        permission_actions = self.check_permissions_default_actions
        self.find_differences(users=users, browsers=browsers,
                              action_tuples=permission_actions)

    def check_downloads(self,
                        browsers='all',
                        users='all',
                        action_tuples=None,
                        item_types='all',
                        click_paths=[None],
                        task=DownloadFiles,
                        urls='all',
                        delete=True):
        """
        Clicks download button and checks download folder for file.
        """
        print('Running check downloads')
        actions = self.check_downloads_default_actions
        admin_only_actions = []
        public_only_actions = []
        browsers, users, item_types, click_paths = self._parse_arguments(browsers=browsers,
                                                                         users=users,
                                                                         item_types=item_types,
                                                                         click_paths=click_paths,
                                                                         action_tuples=action_tuples,
                                                                         actions=actions,
                                                                         admin_only_actions=admin_only_actions,
                                                                         public_only_actions=public_only_actions)
        if urls == 'all':
            urls = [self.prod_url, self.rc_url]
        dm = DataManager(browsers=browsers,
                         urls=urls,
                         users=users,
                         item_types=item_types,
                         click_paths=click_paths,
                         task=task,
                         delete=delete)
        dm.run_tasks()

    @staticmethod
    def _create_authentication_header(headers, authid, authpw):
        headers['Authorization'] = requests.auth._basic_auth_str(
            authid, authpw)
        return headers

    @staticmethod
    def _block_production_edit(url):
        # Don't test production.
        if 'encodeproject.org' in url:
            raise SystemError('No editing production.')
        return None

    def _get_auth_credentials(self, user):
        if user == 'Public':
            return None, None
        cred_file = os.path.expanduser('~/qa_credentials.json')
        with open(cred_file) as f:
            creds = [(k['authid'], k['authpw']) for k
                     in json.load(f) if k['username'] == user]
        if not creds:
            raise ValueError('User not found.')
        return creds[0]

    def check_requests(self):
        """
        Runs post, patch, and get requests as different users.
        """
        url = self.rc_url
        self._block_production_edit(url)
        action_dict = self.check_requests_default_actions
        for k, v in action_dict.items():
            for item in v:
                request_url = urllib.parse.urljoin(url, item[0])
                for user, status_code in item[2]:
                    headers = {'content-type': 'application/json',
                               'accept': 'application/json'}
                    (authid, authpw) = self._get_auth_credentials(user)
                    if authid is not None:
                        headers = self._create_authentication_header(
                            headers, authid, authpw)
                    for payload in item[1]:
                        print('Trying to {} {} with {} as user {}'.format(
                            k, request_url, payload, user))
                        print('Expected: {}'.format(status_code))
                        r = requests.request(
                            k, url=request_url, headers=headers, data=json.dumps(payload))
                        try:
                            print('Actual: {}'.format(r.status_code))
                            assert r.status_code == status_code
                            print('{}{} SUCCESSFUL{}'.format(
                                bcolors.OKBLUE, k.upper(), bcolors.ENDC))
                        except AssertionError:
                            print('{}{} FAILURE\n{}{}'.format(
                                bcolors.FAIL, k.upper(), r.text, bcolors.ENDC))
                        finally:
                            print()

    def _add_rc_to_keypairs(self, url):
        """
        Gets production keypair and copies it to rc_url server.
        """
        keypairs = os.path.expanduser('~/keypairs.json')
        try:
            with open(keypairs, 'r') as f:
                data = json.load(f)
                prod_data = data['prod']
                authid, authpw = prod_data['key'], prod_data['secret']
                data['current_rc'] = {'server': url,
                                      'key': authid,
                                      'secret': authpw}
            # Overwrite.
            with open(keypairs, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(e)
            raise

    def check_tools(self):
        """
        Runs pyencoded-tools against RC.
        """
        url = self.rc_url
        self._block_production_edit(url)
        self._add_rc_to_keypairs(url)
        key = '--key current_rc'
        # Expected outut a very weak check.
        tools = self.check_tools_default_actions
        # Get relative Python executable.
        python_executor = sys.executable
        for tool in tools:
            print('Running {}'.format(tool['name']))
            command = tool['command'].format(
                python_executor, tool['name'], key)
            print(command)
            output = subprocess.check_output(
                command, shell=True).decode('utf-8').strip()
            try:
                assert ((output == tool['expected_output'].strip())
                        or (tool['expected_output'] in output))
                print('{}{} SUCCESSFUL{}'.format(
                    bcolors.OKBLUE, tool['name'].upper(), bcolors.ENDC))
            except AssertionError:
                print('{}{} FAILURE{}'.format(
                    bcolors.FAIL, tool['name'].upper(), bcolors.ENDC))
                print('Expected: ')
                print(tool['expected_output'])
                print('Actual: ')
                print(output)
