from .qancode import QANCODE
from .clickpaths import *

__doc__ = """
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
                        action_tuples=[('/experiments/ENCSR985KAT/', OpenUCSCGenomeBrowserHG19)])

Will open UCSC Genome Browser for hg19 assembly from Experiment page and then
take screenshot to compare.

# Equivalently can pass item_type and click_path instead of action_tuple:
>>> qa.find_differences(browsers=['Chrome'],
                        users=['Public'],
                        item_types=['/experiments/ENCSR985KAT/'],
                        click_paths=[OpenUCSCGenomeBrowserHG19])

Required
--------
Selenium webdriver for Chrome, Firefox.

Safari Technology Preview version.

OpenCV and PIL for Python 3.

To run as any user != Public must create ~/qa_credentials.json file with
list of objects containing username and password fields:

[{"username": "enxxxxtest@gmail.com", "password": "xxxxx"},
 {"username": "encxxxxtest2@gmail.com", "password": "xxxxx"},
 {"username": "encxxxxtest4@gmail.com", "password": "xxxxx"}]

"""
