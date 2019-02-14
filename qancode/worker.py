import time

from .clickpaths import (DownloadBEDFileFromModal,
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
                         OpenUCSCGenomeBrowserMM9fromExperiment)
from .defaults import bcolors
from .tasks import Driver, SignIn

################################################
# Classes for running Selenium tasks robustly. #
################################################


class DataWorker:
    def __init__(self, browser, url, user, task, item_type, click_path, server_name, **kwargs):
        self.task_completed = False
        self.browser = browser
        self.task = task
        self.url = url
        self.user = user
        self.item_type = item_type
        self.click_path = click_path
        self.server_name = server_name
        self.kwargs = kwargs

    def new_driver(self):
        self.driver = Driver(self.browser, self.url).driver

    def run_task(self):
        try:
            self.new_driver()
            if self.user != 'Public':
                signed_in = SignIn(self.driver, self.user)
                print('Refreshing.')
                self.driver.refresh()
                if not signed_in:
                    raise ValueError('Login stalled.')
            new_task = self.task(self.driver,
                                 self.item_type,
                                 self.click_path,
                                 self.server_name,
                                 **self.kwargs)
            data = new_task.get_data()
            self.task_completed = True
            return data
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print('{}Exception caught: {}{}'.format(
                bcolors.FAIL, e, bcolors.ENDC))
        finally:
            try:
                self.driver.quit()
            except AttributeError:
                pass


class DataManager:
    def __init__(self, browsers, urls, users, task, item_types=[None], click_paths=[None], **kwargs):
        self.browsers = browsers
        self.urls = urls
        self.users = users
        self.task = task
        self.item_types = item_types
        self.click_paths = click_paths
        self.all_data = []
        self.kwargs = kwargs

    def run_tasks(self):
        for user in self.users:
            for browser in self.browsers:
                for url in self.urls:
                    for item_type, click_path in zip(self.item_types, self.click_paths):
                        retry = 5
                        while True:
                            if self.urls[0] == url:
                                server_name = 'prod'
                            else:
                                server_name = 'RC'
                            dw = DataWorker(browser=browser,
                                            url=url,
                                            user=user,
                                            task=self.task,
                                            item_type=item_type,
                                            click_path=click_path,
                                            server_name=server_name,
                                            **self.kwargs)
                            data = dw.run_task()
                            if dw.task_completed:
                                self.all_data.append({'browser': browser,
                                                      'url': url,
                                                      'user': user,
                                                      'item_type': item_type,
                                                      'click_path': click_path,
                                                      'data': data,
                                                      'server_name': server_name})
                                break
                            time.sleep(2)
                            retry -= 1
                            if retry < 1:
                                print('{}WARNING: Task incomplete.{}'.format(
                                    bcolors.FAIL, bcolors.ENDC))
                                break
