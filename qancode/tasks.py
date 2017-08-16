import cv2
import getpass
import json
import time
import numpy as np
import os
import pandas as pd
import urllib
import uuid

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from io import BytesIO
from itertools import chain
from PIL import Image
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from tqdm import tqdm

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
                         OpenUCSCGenomeBrowserMM9)
from .defaults import BROWSERS, USERS, bcolors
from .pageobjects import (AntibodyPage,
                          DocumentPreview,
                          DownloadModal,
                          ExperimentPage,
                          FilePage,
                          FrontPage,
                          InformationModal,
                          LoadingSpinner,
                          NavBar,
                          ReportPage,
                          SearchPageList,
                          SearchPageMatrix,
                          SignInModal,
                          UCSCGenomeBrowser,
                          VisualizeModal)


#########################################
# Selenium setup and sign-in procedures. #
#########################################


class Driver:
    """
    Initiate new Selenium driver.
    """

    def __init__(self, browser, url):
        print('Opening {} in {}'.format(url, browser))
        if browser == 'Safari':
            self.driver = webdriver.Safari(
                port=0, executable_path='/Applications/Safari Technology Preview.app/Contents/MacOS/safaridriver')
        elif browser == 'Firefox':
            # Allow automatic downloading of specified MIME types.
            mime_types = 'binary/octet-stream,application/x-gzip,application/gzip,application/pdf,text/plain,text/tsv,image/png,image/jpeg'
            fp = webdriver.FirefoxProfile()
            fp.set_preference(
                'browser.download.manager.showWhenStarting', False)
            fp.set_preference(
                'browser.helperApps.neverAsk.saveToDisk', mime_types)
            fp.set_preference(
                'plugin.disable_full_page_plugin_for_types', mime_types)
            fp.set_preference('pdfjs.disabled', True)
            self.driver = webdriver.Firefox(firefox_profile=fp)
        else:
            self.driver = getattr(webdriver, browser)()
        self.driver.wait = WebDriverWait(self.driver, 5)
        self.driver.wait_long = WebDriverWait(self.driver, 15)
        self.driver.set_window_size(1500, 950)
        self.driver.set_window_position(0, 0)
        self.driver.get(url)

    def driver(self):
        return self.driver


class SignIn:
    """
    Run through OAuth authentication procedure.
    """

    def __init__(self, driver, user, cred_file=os.path.expanduser('~/qa_credentials.json')):
        self.driver = driver
        self.user = user
        self.user_credentials = self.open_credential_file(cred_file)
        self.creds = self.get_credentials_of_user()
        self.sign_in()

    @staticmethod
    def open_credential_file(cred_file):
        with open(cred_file) as f:
            return json.load(f)

    def get_credentials_of_user(self):
        creds = [c for c in self.user_credentials if c['username'] == self.user]
        if len(creds) == 0 and self.user != 'Public':
            raise ValueError('Unknown user')
        else:
            return creds

    def wait_for_modal_to_quit(self):
        wait_time = 10
        while wait_time > 0:
            try:
                WebDriverWait(self.driver, 1).until(EC.presence_of_element_located(
                    (By.CLASS_NAME, SignInModal.login_modal_class)))
                time.sleep(1)
                wait_time -= 1
            except TimeoutException:
                break
        if wait_time < 0:
            raise TimeoutException

    def is_two_step(self):
        if 'stanford' in self.driver.current_url:
            return True
        else:
            return False

    def signed_in(self):
        try:
            self.driver.wait.until(EC.presence_of_element_located(
                (By.LINK_TEXT, FrontPage.logout_button_text)))
            print('Login successful')
            time.sleep(3)
            return True
        except TimeoutError:
            return False

    def login_two_step(self):
        user_id = self.driver.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, SignInModal.two_step_user_id_input_css)))
        user_id.send_keys(self.creds[0]['username'])
        password = self.driver.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, SignInModal.two_step_password_input_css)))
        pw = getpass.getpass()
        password.send_keys(pw)
        pw = None
        submit = self.driver.wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, SignInModal.two_step_submit_css)))
        submit.click()
        send_sms = self.driver.wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, SignInModal.two_step_send_sms_css)))
        send_sms.click()
        code = self.driver.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, SignInModal.two_step_code_input_css)))
        verification = input('Authentication code: ')
        code.send_keys(verification)
        submit = self.driver.wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, SignInModal.two_step_submit_verification_css)))
        submit.click()

    def sign_in(self):
        print('Logging in as {}'.format(self.user))
        original_window_handle = self.driver.window_handles[0]
        self.driver.switch_to_window(original_window_handle)
        login_button = self.driver.wait.until(EC.element_to_be_clickable(
            (By.PARTIAL_LINK_TEXT, FrontPage.login_button_text)))
        try:
            login_button.click()
            self.driver.wait.until(EC.presence_of_element_located(
                (By.CLASS_NAME, SignInModal.login_modal_class)))
        except TimeoutException:
            login_button = self.driver.wait.until(EC.element_to_be_clickable(
                (By.PARTIAL_LINK_TEXT, FrontPage.login_button_text)))
            login_button.click()
        try:
            time.sleep(1)
            google_button = self.driver.wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, SignInModal.google_button_css)))
            google_button.click()
        except TimeoutException:
            # Hack to find button in Safari.
            for button in self.driver.find_elements_by_tag_name(SignInModal.button_tag):
                if '@' in button.text:
                    button.click()
                    break
            self.wait_for_modal_to_quit()
            return None
        try:
            user_id = self.driver.wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, SignInModal.user_id_input_css)))
        except TimeoutException:
            new_window_handle = [h for h in self.driver.window_handles
                                 if h != original_window_handle][0]
            self.driver.switch_to_window(new_window_handle)
            user_id = self.driver.wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, SignInModal.user_id_input_css)))
        user_id.send_keys(self.creds[0]['username'])
        next_button = self.driver.wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, SignInModal.user_next_button_css)))
        next_button.click()
        try:
            pw = self.driver.wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, SignInModal.password_input_css)))
            pw.send_keys(self.creds[0]['password'])
            next_button = self.driver.wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, SignInModal.password_next_button_css)))
            time.sleep(0.5)
            next_button.click()
            time.sleep(0.5)
        except TimeoutException:
            if self.is_two_step():
                self.login_two_step()
            else:
                new_window_handle = [h for h in self.driver.window_handles
                                     if h != original_window_handle][0]
                self.driver.switch_to_window(new_window_handle)
                next_button = self.driver.wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, SignInModal.password_next_button_css)))
                next_button.click()
        self.driver.switch_to_window(original_window_handle)
        return self.signed_in()


#############################################
# Abstract method for data gathering tasks. #
#############################################


class SeleniumTask(metaclass=ABCMeta):
    """
    ABC for defining a Selenium task.
    """

    def __init__(self, driver, item_type, click_path, server_name=None, **kwargs):
        self.driver = driver
        self.item_type = item_type
        self.click_path = click_path
        self.server_name = server_name
        self.temp_dir = kwargs.get('temp_dir', None)
        self.kwargs = kwargs

    def _wait_for_loading_spinner(self):
        if any([y.is_displayed() for y in self.driver.find_elements_by_class_name(LoadingSpinner.loading_spinner_class)]):
            print('Waiting for spinner')
            browser = self.driver.capabilities['browserName'].title()
            for tries in tqdm(range(10)):
                if any([y.is_displayed() for y in self.driver.find_elements_by_class_name(LoadingSpinner.loading_spinner_class)]):
                    time.sleep(1)
                else:
                    print('Loading complete')
                    break
            else:
                print('{} WARNING: Loading spinner still visible'
                      ' on {} in {} after ten seconds.{}'.format(bcolors.FAIL,
                                                                 self.driver.current_url,
                                                                 browser,
                                                                 bcolors.ENDC))
        else:
            print('Loading complete')

    def _try_load_item_type(self):
        time.sleep(2)
        if ((self.item_type is not None)
                and (self.item_type != '/')):
            type_url = self.driver.current_url + self.item_type
            print('Getting type: {}'.format(self.item_type))
            self.driver.get(type_url)
        time.sleep(2)
        self._wait_for_loading_spinner()
        # Pass if not ENCODE page.
        try:
            self.driver.wait.until(
                EC.element_to_be_clickable((By.ID, 'navbar')))
        except:
            pass

    def _try_perform_click_path(self):
        if self.click_path is not None:
            print('Performing click path: {}'.format(self.click_path.__name__))
            return self.click_path(self.driver)

    def _expand_document_details(self):
        expand_buttons = self.driver.wait.until(EC.presence_of_all_elements_located(
            (By.CLASS_NAME, DocumentPreview.document_expand_buttons_class)))
        for button in expand_buttons:
            try:
                button.click()
            except:
                pass

    def _get_rid_of_test_warning_banner(self):
        testing_warning_banner_button = WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, NavBar.testing_warning_banner_button_css)))
        testing_warning_banner_button.click()

    @abstractmethod
    def get_data(self):
        pass


###################
# Selenium tasks. #
###################


class GetFacetNumbers(SeleniumTask):
    """
    Implementation of Task for getting facet number data.
    """

    def matrix_page(self):
        print('Matrix page detected')
        box_top = self.driver.wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, SearchPageMatrix.box_top_css)))
        box_left = self.driver.wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, SearchPageMatrix.box_left_css)))
        see_more_top_buttons = self.driver.find_elements_by_css_selector(
            SearchPageMatrix.see_more_top_buttons_css)
        see_more_left_buttons = self.driver.find_elements_by_css_selector(
            SearchPageMatrix.see_more_left_buttons_css)
        for button in chain(see_more_top_buttons, see_more_left_buttons):
            button.click()
        facets_top = box_top.find_elements_by_class_name(
            SearchPageMatrix.facets_top_class)
        facets_left = box_left.find_elements_by_class_name(
            SearchPageMatrix.facets_left_class)
        facets = chain(facets_top, facets_left)
        return facets

    def search_page(self):
        print('Search page detected')
        facet_box = self.driver.wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, SearchPageList.facet_box_class)))
        see_more_buttons = facet_box.find_elements_by_css_selector(
            SearchPageList.see_more_buttons_css)
        for button in see_more_buttons:
            button.click()
        facets = self.driver.find_elements_by_class_name(
            SearchPageList.facet_class)
        return facets

    def get_data(self):
        if self.item_type is None:
            try:
                data_button = self.driver.wait_long.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, FrontPage.menu_button_data_css)))
            except TimeoutException:
                data_button = self.driver.wait_long.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, FrontPage.menu_button_data_alt_css)))
            data_button.click()
            search_button = self.driver.wait_long.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, FrontPage.drop_down_search_button_css)))
            search_button.click()
        else:
            type_url = self.driver.current_url + self.item_type
            print('Getting type: {}'.format(self.item_type))
            self.driver.get(type_url)
        try:
            self.driver.wait.until(EC.title_contains('Search'))
        except TimeoutException:
            pass
        if 'matrix' in self.driver.current_url:
            facets = self.matrix_page()
        else:
            facets = self.search_page()
        data_dict = defaultdict(list)
        for facet in facets:
            title = facet.find_element_by_css_selector(
                'h5').text.replace(':', '').strip()
            categories = [
                c.text for c in facet.find_elements_by_class_name(SearchPageList.category_title_class)]
            # print('Collecting values in {}.'.format(title))
            numbers = [n.text for n in facet.find_elements_by_class_name(
                SearchPageList.number_class) if n.text != '']
            assert len(categories) == len(numbers)
            if title in data_dict.keys():
                title_number = len([t for t in data_dict.keys()
                                    if t.startswith(title)]) + 1
                title = title + str(title_number)
            data_dict[title] = list(zip(categories, numbers))
        return data_dict


class GetScreenShot(SeleniumTask):
    """
    Get screenshot for comparison.
    """

    def stitch_image(self, image_path):
        print('Stitching screenshot')
        self.driver.execute_script('window.scrollTo(0, {});'.format(0))
        image_slices = []
        while True:
            # Move client_height and scroll_height inside of loop for
            # dynamically expanding pages.
            client_height = self.driver.execute_script(
                'return document.documentElement.clientHeight;')
            scroll_height = self.driver.execute_script(
                'return document.body.scrollHeight;')
            scroll_top = self.driver.execute_script(
                'return document.body.scrollTop || document.documentElement.scrollTop;')
            time.sleep(1)
            image = Image.open(
                BytesIO(self.driver.get_screenshot_as_png())).convert('RGB')
            if ((((2 * client_height) + scroll_top) >= scroll_height)
                    and (scroll_height != (client_height + scroll_top))):
                # Get difference for cropping next image.
                difference_to_keep = abs(
                    2 * (scroll_height - (client_height + scroll_top)))
            if np.allclose(scroll_height, (client_height + scroll_top), rtol=0.0025):
                image = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
                y_bound_low = image.shape[0] - difference_to_keep
                image = image[y_bound_low:, :]
                image_slices.append(image)
                break
            image = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
            image_slices.append(image)
            self.driver.execute_script(
                'window.scrollTo(0, {});'.format(client_height + scroll_top))
            try:
                self.driver.execute_script(
                    'document.getElementById("navbar").style.visibility = "hidden";')
            except:
                pass
        stitched = np.concatenate(*[image_slices], axis=0)
        # Crop stitched rightside a bit so scrollbar doesn't influence diff.
        stitched = stitched[:, :stitched.shape[1] - 20]
        cv2.imwrite(image_path, stitched)

    def take_screenshot(self):
        current_url = self.driver.current_url
        print('Taking picture of {}'.format(current_url))
        filename = '{}{}.png'.format(self.server_name, uuid.uuid4().int)
        image_path = os.path.join(self.temp_dir, filename)
        print(image_path)
        client_height = self.driver.execute_script(
            'return document.documentElement.clientHeight;')
        scroll_height = self.driver.execute_script(
            'return document.body.scrollHeight;')
        if ((self.driver.capabilities['browserName'] == 'safari')
                or (client_height == scroll_height)):
            self.driver.save_screenshot(image_path)
        else:
            self.stitch_image(image_path)
        return image_path

    def make_experiment_pages_look_the_same(self):
        for y in self.driver.wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, ExperimentPage.done_panel_class))):
            try:
                title = y.find_element_by_tag_name(
                    ExperimentPage.title_tag_name).text
                if title == 'Files':
                    graph_close_button = WebDriverWait(y, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, ExperimentPage.graph_close_button_css)))
                    graph_close_button.click()
                    y.find_element_by_xpath(
                        ExperimentPage.sort_by_accession_xpath).click()
            except:
                pass
            if self.click_path != DownloadGraphFromExperimentPage:
                try:
                    self.driver.find_elements_by_xpath(
                        ExperimentPage.file_graph_tab_xpath)[0].click()
                except:
                    pass

    def get_data(self):
        self._try_load_item_type()
        self._try_perform_click_path()
        if 'experiment' in self.driver.current_url:
            try:
                self.make_experiment_pages_look_the_same()
            except:
                pass
        try:
            self._expand_document_details()
        except:
            pass
        try:
            self._get_rid_of_test_warning_banner()
        except:
            pass
        self.driver.execute_script(
            'window.scrollTo(0,document.body.scrollHeight);')
        time.sleep(1)
        self.driver.execute_script('window.scrollTo(0, 0);')
        time.sleep(1)
        image_path = self.take_screenshot()
        return image_path


class DownloadFiles(SeleniumTask):
    """
    Download file given click_path.
    """

    def _get_metadata_from_files_txt(self):
        download_directory = os.path.join(os.path.expanduser('~'), 'Downloads')
        file_path = os.path.join(download_directory, 'files.txt')
        with open(file_path, 'r') as f:
            meta_data_link = f.readline().strip()
            accessions = [f.split('/files/')[1].split('/')[0]
                          for f in f.readlines()]
        # Get metadata.tsv.
        print('Detected {} accessions'.format(len(accessions)))
        print('Getting metadata.tsv from {}'.format(meta_data_link))
        # Workaround for Safari.
        if self.driver.capabilities['browserName'] == 'safari':
            r = urllib.request.urlopen(meta_data_link)
            data = r.read()
            metadata = pd.read_table(BytesIO(data))
        else:
            self.driver.execute_script(
                'window.open("{}");'.format(meta_data_link))
            time.sleep(5)
            metadata_path = os.path.join(download_directory, 'metadata.tsv')
            metadata = pd.read_table(metadata_path)
        if set(accessions) == set(metadata['File accession']):
            print('{}Accessions match: files.txt and metadata.tsv{}'.format(
                bcolors.OKBLUE, bcolors.ENDC))
        else:
            print('{}WARNING: Accession mismatch between files.txt and metadata.tsv{}'.format(
                bcolors.FAIL, bcolors.ENDC))
            print('{}{}{}'.format(bcolors.FAIL, set(accessions).symmetric_difference(
                set(metadata['File accession'])), bcolors.ENDC))
        if self.kwargs.get('delete'):
            try:
                os.remove(metadata_path)
            except:
                pass

    def _check_download_folder(self, filename, download_start_time, results):
        files = os.listdir(os.path.join(
            os.path.expanduser('~'), 'Downloads'))
        for file in files:
            full_path = os.path.join(
                os.path.expanduser('~'), 'Downloads', file)
            # Inexact match to deal with multiple download of same file.
            # Safari overwrites multiple downloads of same file so filepath
            # not unique.
            if ((file.startswith(filename[:len(filename) - 8]))
                    and ((self.driver.capabilities['browserName'] == 'safari')
                         or (file not in [r[3] for r in results]))):
                return (True, full_path, filename, file)
        return (False, full_path, filename, None)

    def _wait_for_download_to_finish(self):
        # Wait for download completion.
        print('Waiting for download to finish')
        while True:
            files = os.listdir(os.path.join(
                os.path.expanduser('~'), 'Downloads'))
            # Check for downloading files from different browsers.
            if (any([('.part' in f) for f in files])
                    or any([('.download' in f) for f in files])
                    or any([('.crdownload' in f) for f in files])):
                time.sleep(5)
            else:
                break

    def _find_downloaded_file(self):
        """
        Checks for downloaded file in Downloads directory.
        """
        time.sleep(2)
        results = []
        self._wait_for_download_to_finish()
        for filename, download_start_time in zip(self.filenames, self.download_start_times):
            results.append(self._check_download_folder(
                filename, download_start_time, results))
        # Clean up.
        for result in results:
            print('Checking for downloaded file {}'.format(result[2]))
            if result[0]:
                print('{}DOWNLOAD SUCCESS: {}{}'.format(
                    bcolors.OKBLUE, result[2], bcolors.ENDC))
                if ((result[2] == 'files.txt')
                        and (self.click_path == DownloadMetaDataFromSearchPage)):
                    self._get_metadata_from_files_txt()
                if self.kwargs.get('delete'):
                    try:
                        os.remove(result[1])
                    except:
                        pass
            else:
                print('{}DOWNLOAD FAILURE: {}{}'.format(
                    bcolors.FAIL, result[2], bcolors.ENDC))

    def get_data(self):
        self._try_load_item_type()
        if self.click_path == DownloadDocumentsFromAntibodyPage:
            self._expand_document_details()
        cp = self._try_perform_click_path()
        self.filenames = cp.filenames
        self.download_start_times = cp.download_start_times
        self._find_downloaded_file()
