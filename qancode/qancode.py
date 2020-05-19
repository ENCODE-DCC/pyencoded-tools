import json
import tempfile
import numpy as np
import os
import requests
import subprocess
import sys
import urllib
import pandas as pd

from clickpaths import (DownloadBEDFileFromModal,
                         DownloadBEDFileFromTable,
                         DownloadDocuments,
                         DownloadDocumentsFromAntibodyPage,
                         DownloadFileFromButton,
                         DownloadFileFromFilePage,
                         DownloadFileFromModal,
                         DownloadFileFromTable,
                         DownloadGraphFromExperimentPage,
                         DownloadMetaDataFromSearchPage,
                         DownloadTSVFromReportPage,
                         OpenUCSCGenomeBrowser,
                         OpenUCSCGenomeBrowserCE10,
                         OpenUCSCGenomeBrowserCE11,
                         OpenUCSCGenomeBrowserDM3,
                         OpenUCSCGenomeBrowserDM6,
                         OpenUCSCGenomeBrowserGRCh38,
                         OpenUCSCGenomeBrowserHG19,
                         OpenUCSCGenomeBrowserMM10,
                         OpenUCSCGenomeBrowserMM10Minimal,
                         OpenUCSCGenomeBrowserMM9,
                         OpenUCSCGenomeBrowserCE10fromExperiment,
                         OpenUCSCGenomeBrowserCE11fromExperiment,
                         OpenUCSCGenomeBrowserDM3fromExperiment,
                         OpenUCSCGenomeBrowserDM6fromExperiment,
                         OpenUCSCGenomeBrowserGRCh38fromExperiment,
                         OpenUCSCGenomeBrowserHG19fromExperiment,
                         OpenUCSCGenomeBrowserMM10fromExperiment,
                         OpenUCSCGenomeBrowserMM10MinimalfromExperiment,
                         OpenUCSCGenomeBrowserMM9fromExperiment,
                         ClickSearchResultItem)
from comparisons import (BrowserComparison,
                          CompareFacetNumbersBetweenBrowsers,
                          CompareFacetNumbersBetweenURLS,
                          CompareScreenShots,
                          URLComparison)
from defaults import (USERS,
                       BROWSERS,
                       bcolors,
                       ActionTuples)
from tasks import (DownloadFiles,
                    Driver,
                    GetFacetNumbers,
                    GetScreenShot,
                    SeleniumTask,
                    SignIn)
from worker import DataWorker, DataManager

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
                                                                 output_directory=None,
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
                         output_directory=None,
                         task=GetScreenShot):
        """
        Does image diff for given item_types. If click_path defined will
        perform that action before taking screenshot.
        """
        # Tuple of (item_type, click_path)
        actions = self.find_differences_default_actions
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
                                                 output_directory=output_directory,
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
                    image_diff_path = os.path.expanduser(
                        '~/Desktop/image_diff/')
                    display(Image(filename='{}{}'.format(image_diff_path,
                                                         out[1])))
                    print('\n')
        except AttributeError:
            print('No image diffs found')

    def check_trackhubs(self, browsers=['Safari'], users=['Public'], action_tuples=None, output_directory=None):
        """
        Runs find_differences() image diff for selected list of trackhub
        actions.
        """
        print('Running check trackhubs')
        trackhub_actions = self.check_trackhubs_default_actions
        if action_tuples is None:
            action_tuples = trackhub_actions
        self.find_differences(users=users, browsers=browsers,
                              action_tuples=action_tuples, output_directory=output_directory)

    def check_permissions(self, browsers=['Safari'], users=['Public'], output_directory=None):
        """
        Runs find_differences() image diff on permission check pages.
        """
        print('Running check permissions')
        permission_actions = self.check_permissions_default_actions
        self.find_differences(users=users, browsers=browsers,
                              action_tuples=permission_actions, output_directory=None)

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

    @staticmethod
    def _color_by_value(value, title):
        # Custom ranges.
        category_dict = {'es_time': {'min': 8.0, 'max': 13.0},
                         'queue_time': {'min': 2.0, 'max': 3.0}}
        # Return global defaults if category title not found.
        min_value = category_dict.get(title, {}).get('min', 150.0)
        max_value = category_dict.get(title, {}).get('max', 400.0)
        # Return color based on value.
        if value < min_value:
            return bcolors.OKBLUE
        elif value >= max_value:
            return bcolors.FAIL
        return bcolors.WARNING

    @staticmethod
    def _get_time_headers(url, n):
        time_headers = []
        for i in range(n):
            r = requests.get(url)
            assert r.status_code == 200
            time_headers.append(r.headers)
        return time_headers

    @staticmethod
    def _parse_header(headers):
        headers_split = [(h.split('=')) for h in headers['X-Stats'].split('&')]
        headers_dict = {h[0]: float(h[1])
                        for h in headers_split if 'time' in h[0]}
        return headers_dict

    @staticmethod
    def _summary_for_category(values):
        return round(np.mean(values) / 1000, 3), round(np.std(values) / 1000, 3), len(values)

    @staticmethod
    def _calculate_total_times(values):
        return [sum(item.values()) for item in values]

    @staticmethod
    def _print_header(url):
        break_size = 80 if len(url) < 70 else len(url) + 20
        print(' {} '.format(url).center(break_size, '-'))

    def _print_results(self, title, mean, std, count):
        print('{}Average {}: {} ± {} ms (n={}){}'.format(self._color_by_value(mean, title),
                                                         title,
                                                         mean,
                                                         std,
                                                         count,
                                                         bcolors.ENDC))

    def _average_time_for_get(self, url, n):
        self._print_header(url)
        time_headers = self._get_time_headers(url, n)
        parsed_headers = [self._parse_header(h) for h in time_headers]
        values_out = {}
        for key in sorted(parsed_headers[0].keys()):
            group_values = [v[key] for v in parsed_headers]
            group_mean, group_std, group_count = self._summary_for_category(
                group_values)
            values_out[key] = (group_mean,group_std)
            self._print_results(key, group_mean, group_std, group_count)
        total_mean, total_std, total_count = self._summary_for_category(
            self._calculate_total_times(parsed_headers))
        values_out['total_time'] = (total_mean,total_std)
        self._print_results('total time', total_mean, total_std, total_count)
        return values_out

    def check_response_time(
        self,
        urls=None,
        item_types=[None],
        n=10,
        output_path=os.path.expanduser('~/Desktop/check_response_time.txt'),
        alt_format=True
    ):
        if urls is None:
            urls = [self.prod_url, self.rc_url]
        print('Checking response time')

        data = {
            'item': [],
            'server': [],
            'n_vals': [n] * 2 * len(item_types),
            'es_time': [],
            'queue_time': [],
            'render_time': [],
            'wsgi_time': [],
            'total_time': [],
            'es_time_stdev': [],
            'queue_time_stdev': [],
            'render_time_stdev': [],
            'wsgi_time_stdev': [],
            'total_time_stdev': []
        }
        response_types = ['es_time', 'queue_time','render_time','wsgi_time','total_time']
        with open(output_path, 'w') as f:
            output_results = {}
            for item in item_types:
                output = {}
                print('\n*** item_type: {}'.format(item))
                for url in urls:
                    data['item'].append(item)
                    data['server'].append(url)
                    if item is not None:
                        url = url + item
                    response = self._average_time_for_get(url, n)
                    for response_type in response_types:
                        try:
                            data[response_type].append(response[response_type][0])
                            data[response_type + '_stdev'].append(response[response_type][1])
                        except KeyError:
                            data[response_type].append(None)
                            data[response_type + '_stdev'].append(None)
                    output[url.split('/')[2]] = response
                output_results[item] = output

            outdf = pd.DataFrame(data)
            outdf.to_csv(path_or_buf=f, sep='\t', mode='a', index=False)

        if alt_format:
            stdev_to_avg_map = {
                'es_time_stdev': 'es_time',
                'queue_time_stdev': 'queue_time',
                'render_time_stdev': 'render_time',
                'wsgi_time_stdev': 'wsgi_time',
                'total_time_stdev': 'total_time'
            }
            server_abbreviated = [s.split('/')[2].split('.encodedcc.org')[0] for s in data['server'][0:2]]
            with open(output_path.split('.txt')[0]+'_alt.txt','w') as f2:
                for item in item_types:
                    f2.write('Query (n={}): {}\n'.format(n,item))
                    col_1 = outdf.loc[(outdf['item'] == item) & (outdf['server'] == data['server'][0]), response_types].T
                    col_1.rename(
                        inplace=True,
                        columns={col_1.columns[0]: server_abbreviated[0]}
                    )
                    col_2 = outdf.loc[(outdf['item'] == item) & (outdf['server'] == data['server'][0]), [s + '_stdev' for s in response_types]].T
                    col_2.rename(
                        inplace=True,
                        columns={col_2.columns[0]: server_abbreviated[0] + '_stdev'},
                        index=stdev_to_avg_map
                    )
                    col_3 = outdf.loc[(outdf['item'] == item) & (outdf['server'] == data['server'][1]), response_types].T
                    col_3.rename(
                        inplace=True,
                        columns={col_3.columns[0]: server_abbreviated[1]}
                    )
                    col_4 = outdf.loc[(outdf['item'] == item) & (outdf['server'] == data['server'][1]), [s + '_stdev' for s in response_types]].T
                    col_4.rename(
                        inplace=True,
                        columns={col_4.columns[0]: server_abbreviated[1] + '_stdev'},
                        index=stdev_to_avg_map
                    )
                    reformat_df = pd.concat([col_1,col_2,col_3,col_4],axis=1)
                    reformat_df['percent_diff'] = reformat_df.iloc[:,[0,2]].pct_change(axis='columns').iloc[:,1].round(3)
                    reformat_df.to_csv(path_or_buf=f2, sep='\t', mode='a')

        return output_results
