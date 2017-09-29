import cv2
import numpy as np
import os
import re

from abc import ABCMeta, abstractmethod

from .defaults import bcolors

##############################################
# Abstract method for data comparison tasks. #
##############################################


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

    def __init__(self, browser, user, prod_url, rc_url, item_type, all_data, click_path=None):
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
        print()
        print(' {} '.format(self.item_type.split('=')[1]).center(80, '-'))
        print('Comparing data between browsers: {}'.format(self.browsers))
        print('As user: {}'.format(self.user))
        print('URL: {}'.format(self.url))
        print('Item type: {}{}{}'.format(
            bcolors.OKGREEN, self.item_type, bcolors.ENDC))
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
        print()
        print(' {} '.format(self.item_type.split('=')[1]).center(80, '-'))
        print('Comparing data between URLs.')
        print('As user: {}'.format(self.user))
        print('Browser: {}'.format(self.browser))
        print('First URL: {}'.format(self.prod_url))
        print('Second URL: {}'.format(self.rc_url))
        print('Item type: {}{}{}'.format(
            bcolors.OKGREEN, self.item_type, bcolors.ENDC))
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
            sub_name = re.sub(
                '[/?=&+.%]', '_', self.item_type).replace('__', '_')
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
            # cv2.imwrite(os.path.join(directory, 'match_one.png'), image_one)
            # cv2.imwrite(os.path.join(directory, 'match_two.png'), image_two)
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
        try:
            result = self.compute_image_difference()
            print('Distance metric: {}'.format(self.diff_distance_metric))
            return result
        except IndexError:
            print('{}COMPARISON ERROR. SKIPPING.{}'.format(bcolors.FAIL, bcolors.ENDC))
            return ('FAIL', self.item_type)
    
