import cv2
import getpass
import json
import tempfile
import time
import numpy as np
import os
import uuid

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from io import BytesIO
from itertools import chain
from PIL import Image
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.common.by import By
from tqdm import tqdm


"""
Purpose
-------
Provide robust framework for performing browser tasks with Selenium.

New data gathering tasks can inherit from the SeleniumTask class.

New data comparison tasks between production data and RC data can inherit
from the URLComparison class.

New data comparison tasks between browsers for the same URL can inherit
from the BrowserComparison class.

The DataManager class launches DataWorkers to complete individual tasks
in a fault-tolerant way.

The QANCODE object organizes data gathering tasks and data comparison
tasks into a complete pipeline that can be called with a single
method, e.g. QANCODE.compare_facets().


Example
-------
# Run qancode in interactive Python session.
$ python -i qancode.py

# Initiate QANCODE object with URL to compare to production.
>>> qa = QANCODE(rc_url='https://test.encodedcc.org')

First:

# Run facet comparison for Experiment items in Safari as public and
# admin user.
>>> qa.compare_facets(users=['Public', 'encxxxtest@gmail.com'],
                      browsers=['Safari'],
                      item_types=['/search/?type=Experiment'])

Will return comparison of data between production and RC for a given browser
as well as comparison of data between browsers for a given URL.

Second:

# Find the difference between two screenshots.
>>> qa.find_differences(browsers=['Safari'],
                        users=['Public'],
                        item_types=['/biosamples/ENCBS632MTU/'])

Will output image showing difference if found.

# Perform action before taking screenshot.
>>> qa.find_differences(browsers=['Chrome'],
                        users=['Public'],
                        action_tuples=[('/experiments/ENCSR985KAT/', OpenUCSCGenomeBrowserFromExperimentPageHG19)])

Will open UCSC Genome Browser for hg19 assembly from Experiment page and then
take screenshot to compare.

# Equivalently can pass item_type and click_path instead of action_tuple:
>>> qa.find_differences(browsers=['Chrome'],
                        users=['Public'],
                        item_types=['/experiments/ENCSR985KAT/'],
                        click_paths=[OpenUCSCGenomeBrowserFromExperimentPageHG19])


Required
--------
Selenium webdriver for Chrome, Firefox.

Safari Technology Preview version.

OpenCV for Python 3.

To run as any user != Public must create ~/qa_credentials.json file with
list of objects containing username and password fields:

[{"username": "enxxxxtest@gmail.com", "password": "xxxxx"},
 {"username": "encxxxxtest2@gmail.com", "password": "xxxxx"},
 {"username": "encxxxxtest4@gmail.com", "password": "xxxxx"}]

"""

# Default browsers.
BROWSERS = ['Chrome',
            'Firefox',
            'Safari']

# Default users.
USERS = ['Public',
         'encoded.test@gmail.com',
         'encoded.test2@gmail.com',
         'encoded.test4@gmail.com']


class bcolors:
    """
    Helper class for text color definitions.
    """
    OKBLUE = '\x1b[36m'
    OKGREEN = '\x1b[1;32m'
    WARNING = '\x1b[33m'
    FAIL = '\x1b[31m'
    ENDC = '\x1b[0m'


#######################
# Page object models. #
#######################


class FrontPage(object):
    """
    Page object models allow selectors to be defined in one place and then
    used by all of the test.
    """
    login_button_text = 'Submitter sign-in'
    logout_button_text = 'Submitter sign out'
    menu_button_data_css = '#main > ul > li:nth-child(1) > a'
    menu_button_data_alt_css = '#main > ul > li:nth-child(1) > button'
    drop_down_search_button_css = '#main > ul > li.dropdown.open > ul > li:nth-child(2) > a'


class SignInModal(object):
    """
    Page object model.
    """
    login_modal_class = 'auth0-lock-header-logo'
    google_button_css = '#auth0-lock-container-1 > div > div.auth0-lock-center > form > div > div > div:nth-child(3) > span > div > div > div > div > div > div > div > div > div > div.auth0-lock-social-buttons-container > button:nth-child(2) > div.auth0-lock-social-button-icon'
    button_tag = 'button'
    user_id_input_css = '#identifierId'
    user_next_button_css = '#identifierNext > content > span'
    password_input_css = '#password > div.aCsJod.oJeWuf > div > div.Xb9hP > input'
    password_next_button_css = '#passwordNext > content > span'
    two_step_user_id_input_css = '#username'
    two_step_password_input_css = '#password'
    two_step_submit_css = '#login > input'
    two_step_send_sms_css = '#sms-send > input.submit-button'
    two_step_code_input_css = '#otp'
    two_step_submit_verification_css = '#otp-box > div > input.go-button'


class SearchPageList(object):
    """
    Page object model.
    """
    facet_box_class = 'facets'
    see_more_buttons_css = '#content > div > div > div > div > div.col-sm-5.col-md-4.col-lg-3 > div > div > div > ul > div.pull-right > small > button'
    facet_class = 'facet'
    category_title_class = 'facet-item'
    number_class = 'pull-right'


class SearchPageMatrix(object):
    """
    Page object model.
    """
    see_more_top_buttons_css = '#content > div > div > div > div.col-sm-7.col-md-8.col-lg-9.sm-no-padding > div > div > div > ul > div.pull-right > small > button'
    see_more_left_buttons_css = '#content > div > div > div > div.col-sm-5.col-md-4.col-lg-3.sm-no-padding > div > div > div > ul > div.pull-right > small > button'
    box_top_css = '#content > div > div > div:nth-child(1) > div.col-sm-7.col-md-8.col-lg-9.sm-no-padding > div'
    box_left_css = '#content > div > div > div:nth-child(2) > div.col-sm-5.col-md-4.col-lg-3.sm-no-padding > div'
    facets_top_class = 'facet'
    facets_left_class = 'facet'


class ExperimentPage(object):
    """
    Page object model.
    """
    done_panel_class = 'done'
    title_tag_name = 'h4'
    graph_close_button_css = 'div > div:nth-child(2) > div.file-gallery-graph-header.collapsing-title > button'
    sort_by_accession_x_path = '//div/div[3]/div[2]/div/table[2]/thead/tr[2]/th[1]'
    all_buttons_tag_name = 'button'


class VisualizeModal(object):
    """
    Page object model.
    """
    modal_class = 'modal-content'
    UCSC_link_partial_link_text = 'UCSC'


class NavBar(object):
    """
    Page object model.
    """
    testing_warning_banner_button_css = '#navbar > div.test-warning > div > p > button'


class LoadingSpinner(object):
    """
    Page object model.
    """
    loading_spinner_class = 'loading-spinner'


class DocumentPreview(object):
    """
    Page object model.
    """
    document_expand_buttons_class = 'document__file-detail-switch'


class UCSCGenomeBrowser(object):
    """
    Page object model.
    """
    zoom_one_id = 'hgt.in1'

##################################################################
# Abstract methods for data gathering and data comparison tasks. #
##################################################################


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

    @abstractmethod
    def get_data(self):
        pass


class BrowserComparison(metaclass=ABCMeta):
    """
    ABC for comparing data between browsers.
    """

    def __init__(self, user, url, item_type, browsers, all_data):
        self.all_data = all_data
        self.user = user
        self.url = url
        self.item_type = item_type
        self.browsers = browsers
        self.url_data = [d for d in all_data if ((d['user'] == user)
                                                 and (d['item_type'] == item_type)
                                                 and (d['url'] == url))]

    @abstractmethod
    def compare_data(self):
        pass


class URLComparison(metaclass=ABCMeta):
    """
    ABC for comparing data between prod and RC given browser and user.
    """

    def __init__(self, browser, user, prod_url, rc_url, item_type, click_path, all_data):
        self.browser = browser
        self.user = user
        self.all_data = all_data
        self.prod_url = prod_url
        self.rc_url = rc_url
        self.item_type = item_type
        self.click_path = click_path
        self.prod_data = [d['data'] for d in all_data
                          if ((d['url'] == prod_url)
                              and (d['user'] == user)
                              and (d['browser'] == browser)
                              and (d['item_type'] == item_type)
                              and (d['click_path'] == click_path))]
        self.rc_data = [d['data'] for d in all_data
                        if ((d['url'] == rc_url)
                            and (d['user'] == user)
                            and (d['browser'] == browser)
                            and (d['item_type'] == item_type)
                            and (d['click_path'] == click_path))]
        assert len(self.prod_data) == len(self.rc_data)

    @abstractmethod
    def compare_data(self):
        pass

#########################################
# Selenium setup and sign-in procedures. #
#########################################


class NewDriver(object):
    """
    Initiate new Selenium driver.
    """

    def __init__(self, browser, url):
        print('Opening {} in {}'.format(url, browser))
        if browser == 'Safari':
            self.driver = webdriver.Safari(
                port=0, executable_path='/Applications/Safari Technology Preview.app/Contents/MacOS/safaridriver')
        else:
            self.driver = getattr(webdriver, browser)()
        self.driver.wait = WebDriverWait(self.driver, 5)
        self.driver.wait_long = WebDriverWait(self.driver, 15)
        self.driver.set_window_size(1500, 950)
        self.driver.set_window_position(0, 0)
        self.driver.get(url)

    def driver(self):
        return self.driver


class SignIn(object):
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
        # self.wait_for_modal_to_quit()
        # self.driver.switch_to_window(original_window_handle)
        return self.signed_in()


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
            print('Collecting values in {}.'.format(title))
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
        client_height = self.driver.execute_script(
            'return document.documentElement.clientHeight;')
        scroll_height = self.driver.execute_script(
            'return document.body.scrollHeight;')
        image_slices = []
        while True:
            scroll_top = self.driver.execute_script(
                'return document.body.scrollTop || document.documentElement.scrollTop;')
            time.sleep(1)
            image = Image.open(
                BytesIO(self.driver.get_screenshot_as_png())).convert('RGB')
            if ((((2 * client_height) + scroll_top) > scroll_height)
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
                        ExperimentPage.sort_by_accession_x_path).click()
            except:
                pass

    def expand_document_details(self):
        expand_buttons = self.driver.wait.until(EC.presence_of_all_elements_located(
            (By.CLASS_NAME, DocumentPreview.document_expand_buttons_class)))
        for button in expand_buttons:
            try:
                button.click()
            except:
                pass

    def get_rid_of_test_warning_banner(self):
        testing_warning_banner_button = WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, NavBar.testing_warning_banner_button_css)))
        testing_warning_banner_button.click()

    def wait_for_loading_spinner(self):
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
                      ' on {} in {} after ten seconds. Taking screenshot.{}'.format(bcolors.FAIL,
                                                                                    self.driver.current_url,
                                                                                    browser,
                                                                                    bcolors.ENDC))
        else:
            print('Loading complete')

    def get_data(self):
        time.sleep(2)
        if ((self.item_type is not None)
                and (self.item_type != '/')):
            type_url = self.driver.current_url + self.item_type
            print('Getting type: {}'.format(self.item_type))
            self.driver.get(type_url)
        time.sleep(2)
        self.wait_for_loading_spinner()
        self.driver.wait.until(
            EC.element_to_be_clickable((By.ID, 'navbar')))
        if self.click_path is not None:
            print('Performing click path: {}'.format(self.click_path.__name__))
            self.click_path(self.driver)
        if 'experiment' in self.driver.current_url:
            try:
                self.make_experiment_pages_look_the_same()
            except:
                pass
        try:
            self.expand_document_details()
        except:
            pass
        try:
            self.get_rid_of_test_warning_banner()
        except:
            pass
        self.driver.execute_script(
            'window.scrollTo(0,document.body.scrollHeight);')
        time.sleep(1)
        self.driver.execute_script('window.scrollTo(0, 0);')
        time.sleep(1)
        image_path = self.take_screenshot()
        return image_path


##########################
# Data comparison tasks. #
##########################


class CompareFacetNumbersBetweenBrowsers(BrowserComparison):
    """
    Implementation of BrowserComparison for facet numbers.
    """

    def compare_data(self):
        """
        Return comparison of data between browsers given server (prod/RC),
        user, item_type.
        """
        print('Comparing data between browsers: {}.'.format(self.browsers))
        print('As user: {}'.format(self.user))
        print('URL: {}'.format(self.url))
        print('Item type: {}'.format(self.item_type))
        # Find keys that are not in all groups.
        all_keys = set.union(*[set(d['data'].keys()) for d in self.url_data])
        common_keys = set.intersection(*[set(d['data'].keys())
                                         for d in self.url_data])
        different_keys = all_keys - common_keys
        if different_keys:
            for key in different_keys:
                print(key)
                # Print groups that have key.
                browsers_with_key = set([d['browser'] for d in self.url_data
                                         if key in d['data'].keys()])
                if browsers_with_key:
                    print('{}{}In browsers: {}{}'.format(
                        ' ' * 5, bcolors.WARNING, list(browsers_with_key), bcolors.ENDC))
                # Print groups that do not have key.
                browsers_without_key = set(
                    [d['browser'] for d in self.url_data if key not in d['data'].keys()])
                if browsers_without_key:
                    print('{}{}Not in browsers: {}{}'.format(
                        ' ' * 5, bcolors.FAIL, list(browsers_without_key), bcolors.ENDC))
        if common_keys:
            for key in sorted(common_keys):
                print(key)
                category_data_by_browser = [(d['browser'], set(d['data'][key]))
                                            for d in self.url_data]
                all_data = set.union(*[d[1] for d in category_data_by_browser])
                common_data = set.intersection(*[d[1] for d
                                                 in category_data_by_browser])
                different_data = all_data - common_data
                if different_data:
                    for dd in different_data:
                        browsers_with_different_data = [
                            d[0] for d in category_data_by_browser if dd in d[1]]
                        print('{}{}{}{}'.format(
                            ' ' * 5, bcolors.OKGREEN, dd, bcolors.ENDC))
                        print('{}{}In browsers: {}{}'.format(
                            ' ' * 10, bcolors.WARNING, list(browsers_with_different_data), bcolors.ENDC))
                        browsers_without_different_data = [
                            d[0] for d in category_data_by_browser if dd not in d[1]]
                        print('{}{}Not in browsers: {}{}'.format(
                            ' ' * 10, bcolors.FAIL, list(browsers_without_different_data), bcolors.ENDC))
                else:
                    print('{}{}MATCH{}'.format(
                        ' ' * 5, bcolors.OKBLUE, bcolors.ENDC))


class CompareFacetNumbersBetweenURLS(URLComparison):
    """
    Implementation of URLComparison for facet numbers.
    """

    def compare_data(self):
        print('Comparing data between URLs.')
        print('As user: {}'.format(self.user))
        print('Browser: {}'.format(self.browser))
        print('First URL: {}'.format(self.prod_url))
        print('Second URL: {}'.format(self.rc_url))
        print('Item type: {}'.format(self.item_type))
        prod_data = self.prod_data[0]
        rc_data = self.rc_data[0]
        if prod_data.keys() != rc_data.keys():
            print('Different keys:')
            in_prod = prod_data.keys() - rc_data.keys()
            in_rc = rc_data.keys() - prod_data.keys()
            if in_prod:
                print('RC missing: {}'.format(in_prod))
            if in_rc:
                print('Production missing: {}'.format(in_rc))
        for title in sorted(set(prod_data.keys()).union(set(rc_data.keys()))):
            prod = set(prod_data[title])
            rc = set(rc_data[title])
            if prod != rc:
                in_prod = sorted(prod - rc)
                in_rc = sorted(rc - prod)
                print(title.upper())
                if ((len(in_prod) == len(in_rc))
                        and (set([k[0] for k in prod_data[title]]) == set([k[0] for k in rc_data[title]]))):
                    for p, r in zip(in_prod, in_rc):
                        print('{}{}{}: {} (prod), {} (rc){}'.format(
                            ' ' * 5, bcolors.FAIL, p[0], p[1], r[1], bcolors.ENDC))
                else:
                    both_keys = set([x[0] for x in in_prod]).intersection(
                        set([x[0] for x in in_rc]))
                    both_prod = sorted(
                        [x for x in in_prod if x[0] in both_keys])
                    both_rc = sorted([x for x in in_rc if x[0] in both_keys])
                    if both_prod:
                        for p, r in zip(both_prod, both_rc):
                            print('{}{}{}: {} (prod), {} (rc){}'.format(
                                ' ' * 5, bcolors.FAIL, p[0], p[1], r[1], bcolors.ENDC))
                    only_prod = [x for x in in_prod if x[0] not in both_keys]
                    if only_prod:
                        print('{}{}prod: {}{}'.format(
                            ' ' * 5, bcolors.WARNING, only_prod, bcolors.ENDC))
                    only_rc = [x for x in in_rc if x[0] not in both_keys]
                    if only_rc:
                        print('{}{}rc: {}{}'.format(
                            ' ' * 5, bcolors.WARNING, only_rc, bcolors.ENDC))
            else:
                print(title)
                print('{}{}MATCH{}'.format(' ' * 5, bcolors.OKBLUE, bcolors.ENDC))


class CompareScreenShots(URLComparison):
    def is_same(self, difference):
        self.diff_distance_metric = difference.sum()
        if np.any(difference):
            # Thresholded value.
            if difference.sum() > 50000:
                return False
        return True

    def pad_if_different_shape(self, image_one, image_two):
        image_one_row_number = image_one.shape[0]
        image_two_row_number = image_two.shape[0]
        pad_shape = abs(image_one_row_number - image_two_row_number)
        if image_one_row_number > image_two_row_number:
            image_two = np.pad(
                image_two, ((0, pad_shape), (0, 0), (0, 0)), mode='constant')
        else:
            image_one = np.pad(
                image_one, ((0, pad_shape), (0, 0), (0, 0)), mode='constant')
        return image_one, image_two

    def compute_image_difference(self):
        directory = os.path.join(
            os.path.expanduser('~'), 'Desktop', 'image_diff')
        if not self.item_type.endswith('/'):
            self.item_type = self.item_type + '/'
        if len(self.item_type) <= 1:
            sub_name = '_front_page_'
        else:
            sub_name = self.item_type.replace(
                '/', '_').replace('?', '_').replace('=', '_').replace('&', '_').replace('__', '_')
        user_name = self.user.split('@')[0].replace('.', '_').lower()
        if not os.path.exists(directory):
            print('Creating directory on Desktop')
            os.makedirs(directory)
        click_path = None if self.click_path is None else self.click_path.__name__
        path_name = '{}{}{}_{}_prod_rc_diff.png'.format(
            self.browser.lower(), sub_name.upper(), user_name, click_path)
        image_one = cv2.imread(self.prod_data[0])
        image_two = cv2.imread(self.rc_data[0])
        if image_one.shape[0] != image_two.shape[0]:
            image_one, image_two = self.pad_if_different_shape(
                image_one, image_two)
        difference = cv2.subtract(image_one, image_two)
        if not self.is_same(difference):
            self.diff_found = True
            print('{}Difference detected{}'.format(bcolors.FAIL, bcolors.ENDC))
            print('{}Outputting file {}{}'.format(
                bcolors.FAIL, path_name, bcolors.ENDC))
            diff = cv2.addWeighted(image_one, 0.2, difference, 1, 0)
            all_viz = np.concatenate([image_one, diff, image_two], axis=1)
            cv2.imwrite(os.path.join(directory, path_name), all_viz)
        else:
            self.diff_found = False
            #cv2.imwrite(os.path.join(directory, 'match_one.png'), image_one)
            #cv2.imwrite(os.path.join(directory, 'match_two.png'), image_two)
            print('{}MATCH{}'.format(bcolors.OKBLUE, bcolors.ENDC))
        return (self.diff_found, path_name)

    def compare_data(self):
        print('\nComparing screenshots between URLs.')
        print('As user: {}'.format(self.user))
        print('Browser: {}'.format(self.browser))
        print('First URL: {}'.format(self.prod_url))
        print('Second URL: {}'.format(self.rc_url))
        print('Item type: {}'.format(self.item_type))
        print('Click path: {}'.format(
            None if self.click_path is None else self.click_path.__name__))
        result = self.compute_image_difference()
        print('Distance metric: {}'.format(self.diff_distance_metric))
        return result


################
# Click paths. #
################

class OpenUCSCGenomeBrowserFromExperimentPage(object):
    """
    Defines clicks required to open UCSC trackhub from Experiment page for
    given assembly.
    """

    def __init__(self, driver, assembly):
        self.driver = driver
        self.assembly = assembly
        self.perform_action()

    def perform_action(self):
        current_window = self.driver.current_window_handle
        time.sleep(1)
        for y in self.driver.find_elements_by_tag_name(ExperimentPage.all_buttons_tag_name):
            try:
                if y.text == 'Visualize':
                    y.click()
                    self.driver.wait.until(EC.element_to_be_clickable(
                        (By.CLASS_NAME, VisualizeModal.modal_class)))
                    break
            except:
                pass
        modal = self.driver.wait.until(
            EC.element_to_be_clickable((By.CLASS_NAME, VisualizeModal.modal_class)))
        UCSC_links = modal.find_elements_by_partial_link_text(
            VisualizeModal.UCSC_link_partial_link_text)
        for link in UCSC_links:
            if self.assembly in link.get_attribute("href"):
                print('Opening genome browser')
                link.click()
                break
        time.sleep(1)
        self.driver.switch_to_window([h for h in self.driver.window_handles
                                      if h != current_window][0])
        self.driver.wait.until(EC.element_to_be_clickable(
            (By.ID, UCSCGenomeBrowser.zoom_one_id)))


class OpenUCSCGenomeBrowserFromExperimentPageGRCh38(object):
    """
    Opens UCSC browser with GRCh38 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'hg38')


class OpenUCSCGenomeBrowserFromExperimentPageHG19(object):
    """
    Opens UCSC browser with hg19 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'hg19')


class OpenUCSCGenomeBrowserFromExperimentPageMM9(object):
    """
    Opens UCSC browser with mm9 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'mm9')


class OpenUCSCGenomeBrowserFromExperimentPageMM10(object):
    """
    Opens UCSC browser with mm10 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'mm10')


class OpenUCSCGenomeBrowserFromExperimentPageMM10Minimal(object):
    """
    Opens UCSC browser with mm10-minimal assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'mm10')


class OpenUCSCGenomeBrowserFromExperimentPageDM3(object):
    """
    Opens UCSC browser with dm3 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'dm3')


class OpenUCSCGenomeBrowserFromExperimentPageDM6(object):
    """
    Opens UCSC browser with dm6 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'dm6')


class OpenUCSCGenomeBrowserFromExperimentPageCE10(object):
    """
    Opens UCSC browser with ce10 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'ce10')


class OpenUCSCGenomeBrowserFromExperimentPageCE11(object):
    """
    Opens UCSC browser with ce11 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperimentPage(driver, 'ce11')


################################################
# Classes for running Selenium tasks robustly. #
################################################


class DataWorker(object):
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
        self.driver = NewDriver(self.browser, self.url).driver

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
            print('Exception caught: {}.'.format(e))
        finally:
            try:
                self.driver.quit()
            except AttributeError:
                pass


class DataManager(object):
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
                        retry = 10
                        while retry > 0:
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
                            if retry < 0:
                                raise ValueError('Task incomplete.')


################################################################
# QANCODE object gets data, compares data using defined tasks. #
################################################################


class QANCODE(object):
    """
    Object to keep track of Task/Comparison combinations and run QA
    process with one method call.
    """

    def __init__(self, rc_url, prod_url='https://encodeproject.org'):
        self.rc_url = rc_url
        self.prod_url = prod_url
        self.browsers = [b for b in BROWSERS]
        self.users = [u for u in USERS]

    def list_methods(self):
        """
        List all possible tests.
        """
        print(*[f for f in dir(QANCODE) if callable(getattr(QANCODE, f))
                and not f.startswith('__')], sep='\n')

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
        all_item_types = [
            '/search/?type=Experiment',
            '/search/?type=File',
            '/search/?type=AntibodyLot',
            '/search/?type=Biosample',
            '/search/?type=Dataset',
            '/search/?type=FileSet',
            '/search/?type=Annotation',
            '/search/?type=Series',
            '/search/?type=OrganismDevelopmentSeries',
            '/search/?type=UcscBrowserComposite',
            '/search/?type=ReferenceEpigenome',
            '/search/?type=Project',
            '/search/?type=ReplicationTimingSeries',
            '/search/?type=PublicationData',
            '/search/?type=MatchedSet',
            '/search/?type=TreatmentConcentrationSeries',
            '/search/?type=TreatmentTimeSeries',
            '/search/?type=Target',
            '/search/?type=Pipeline',
            '/search/?type=Publication',
            '/search/?type=Software',
            '/matrix/?type=Experiment',
            '/matrix/?type=Annotation'
        ]
        if browsers == 'all':
            browsers = self.browsers
        if users == 'all':
            users = self.users
        if item_types == 'all':
            item_types = [t for t in all_item_types]
        urls = [self.prod_url, self.rc_url]
        dm = DataManager(browsers=browsers,
                         urls=urls,
                         users=users,
                         item_types=item_types,
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
        actions = [('/', None),
                   ('/targets/?status=deleted', None),
                   ('/antibodies/?status=deleted', None),
                   ('/search/?type=Biosample&status=deleted', None),
                   ('/experiments/ENCSR000CWD/', None),
                   ('/biosamples/ENCBS574ZRE/', None),
                   ('/biosamples/ENCBS883DWI/', None),
                   ('/experiments/ENCSR985KAT/', None),
                   ('/human-donors/ENCDO999JZG/', None),
                   ('/biosamples/ENCBS615YKY/', None),
                   ('/search/?searchTerm=puf60&type=Target', None),
                   ('/experiments/ENCSR502NRF/', None),
                   ('/experiments/ENCSR000AEH/', None),
                   ('/search/?searchTerm=ENCSR000AEH&type=Experiment', None),
                   ('/experiments/ENCSR000CPG/', None),
                   ('/search/?searchTerm=ENCSR000CPG&type=Experiment', None),
                   ('/experiments/ENCSR000BPF/', None),
                   ('/search/?searchTerm=ENCSR000BPF&type=Experiment', None),
                   ('/experiments/ENCSR178NTX/', None),
                   ('/experiments/ENCSR651NGR/', None),
                   ('/search/?searchTerm=ENCSR651NGR&type=Experiment', None),
                   ('/antibodies/ENCAB000AEH/', None),
                   ('/search/?searchTerm=ENCAB000AEH&type=AntibodyLot', None),
                   ('/antibodies/ENCAB000AIW/', None),
                   ('/search/?searchTerm=ENCAB000AIW&type=AntibodyLot', None),
                   ('/biosamples/ENCBS000AAA/', None),
                   ('/search/?searchTerm=ENCBS000AAA&type=Biosample', None),
                   ('/biosamples/ENCBS030ENC/', None),
                   ('/search/?searchTerm=ENCBS030ENC', None),
                   ('/biosamples/ENCBS098ENC/', None),
                   ('/search/?searchTerm=ENCBS098ENC&type=Biosample', None),
                   ('/biosamples/ENCBS619ENC/', None),
                   ('/search/?searchTerm=ENCBS619ENC&type=Biosample', None),
                   ('/biosamples/ENCBS286AAA/', None),
                   ('/search/?searchTerm=ENCBS286AAA&type=Biosample', None),
                   ('/biosamples/ENCBS314VPT/', None),
                   ('/search/?searchTerm=ENCBS314VPT&type=Biosample', None),
                   ('/biosamples/ENCBS808BUA/', None),
                   ('/search/?searchTerm=ENCBS808BUA&type=Biosample', None),
                   ('/targets/AARS-human/', None),
                   ('/targets/FLAG-GABP-human/', None),
                   ('/search/?type=Target&name=AARS-human', None),
                   ('/ucsc-browser-composites/ENCSR707NXZ/', None),
                   ('/treatment-time-series/ENCSR210PYP/', None),
                   ('/search/?searchTerm=WASP&type=Software', None),
                   ('/publications/67e606ae-abe7-4510-8ebb-cfa1fefe5cfa/', None),
                   ('/search/?searchTerm=PMID%3A25164756', None),
                   ('/biosamples/ENCBS632MTU/', None),
                   ('/biosamples/ENCBS464EKT/', None),
                   ('/annotations/ENCSR790GQB/', None),
                   ('/publications/b2e859e6-3ee7-4274-90be-728e0faaa8b9/', None),
                   ('/pipelines/', None),
                   ('/pipelines/ENCPL210QWH/', None),
                   ('/pipelines/ENCPL002LPE/', None),
                   ('/pipelines/ENCPL002LSE/', None),
                   ('/rna-seq/long-rnas/', None),
                   ('/pipelines/ENCPL337CSA/', None),
                   ('/rna-seq/small-rnas/', None),
                   ('/pipelines/ENCPL444CYA/', None),
                   ('/microrna/microrna-seq/', None),
                   ('/pipelines/ENCPL278BTI/', None),
                   ('/microrna/microrna-counts/', None),
                   ('/pipelines/ENCPL122WIM/', None),
                   ('/rampage/', None),
                   ('/pipelines/ENCPL220NBH/', None),
                   ('/pipelines/ENCPL272XAE/', None),
                   ('/pipelines/ENCPL272XAE/', None),
                   ('/chip-seq/histone/', None),
                   ('/pipelines/ENCPL138KID/', None),
                   ('/pipelines/ENCPL493SGC/', None),
                   ('/chip-seq/transcription_factor/', None),
                   ('/pipelines/ENCPL001DNS/', None),
                   ('/pipelines/ENCPL002DNS/', None),
                   ('/data-standards/dnase-seq/', None),
                   ('/atac-seq/', None),
                   ('/pipelines/ENCPL985BLO/', None),
                   ('/data/annotations/', None),
                   ('/help/rest-api/', None),
                   ('/about/experiment-guidelines/', None),
                   ('/data-standards/terms/', None),
                   ('/experiments/ENCSR502NRF/',
                    OpenUCSCGenomeBrowserFromExperimentPageGRCh38),
                   ('/experiments/ENCSR502NRF/',
                    OpenUCSCGenomeBrowserFromExperimentPageHG19),
                   ('/experiments/ENCSR985KAT/',
                    OpenUCSCGenomeBrowserFromExperimentPageHG19),
                   ('/experiments/ENCSR426UUG/',
                    OpenUCSCGenomeBrowserFromExperimentPageGRCh38),
                   ('/experiments/ENCSR293WTN/',
                    OpenUCSCGenomeBrowserFromExperimentPageMM9),
                   ('/experiments/ENCSR335LKF/',
                    OpenUCSCGenomeBrowserFromExperimentPageMM10),
                   ('/experiments/ENCSR066DPD/',
                    OpenUCSCGenomeBrowserFromExperimentPageMM10Minimal),
                   ('/experiments/ENCSR922ESH/',
                    OpenUCSCGenomeBrowserFromExperimentPageDM3),
                   ('/experiments/ENCSR671XAK/',
                    OpenUCSCGenomeBrowserFromExperimentPageDM6),
                   ('/experiments/ENCSR422XRE/',
                    OpenUCSCGenomeBrowserFromExperimentPageCE10),
                   ('/experiments/ENCSR686FKU/', OpenUCSCGenomeBrowserFromExperimentPageCE11)]
        admin_only_actions = [('/biosamples/ENCBS681LAC/', None),
                              ('/search/?searchTerm=ENCBS681LAC&type=Biosample', None)]
        public_only_actions = [('/experiments/?status=deleted', None)]
        if browsers == 'all':
            browsers = self.browsers
        if users == 'all':
            users = self.users
        if action_tuples is not None:
            item_types, click_paths = self._expand_action_list(action_tuples)
        else:
            if item_types == 'all':
                item_types, click_paths = self._expand_action_list(actions)
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
        return results
