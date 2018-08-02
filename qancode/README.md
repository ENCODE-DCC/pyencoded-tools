# Installation
```
brew cask install chromedriver
mkdir ~/qancode && cd ~/qancode
python3 -m venv venv-name
venv-name/bin/activate
git clone git@github.com:ENCODE-DCC/pyencoded-tools.git
pip install -r ./pyencoded-tools/qancode/requirements.txt
pip install -e ./pyencoded-tools/qancode
```
# Basic Test Scriopt
```
from qancode import QANCODE

demo_url = 'https://your-demo-url-name.demo.encodedcc.org'
test_against_url = 'https://test.encodedcc.org'
qan = QANCODE(demo_url, prod_url=test_against_url)
qan.list_methods()
# qan.compare_facets() # Start a certain test
```


## Purpose
_qancode_ automates ENCODE release testing (QA) tasks that may require manual review.

## Examples
Run _qancode_ package in interactive Python session:
```bash
$ python -i qancode
```

Initiate QANCODE object with URL to compare to production:
```python
 qa = QANCODE(rc_url='https://test.encodedcc.org')
```


Compare search facets as public user in Safari:
```python
qa.compare_facets(users=['Public'],
                  browsers=['Safari'],
                  item_types=['/search/?type=Experiment'])
```


Find difference between two screenshots:
```python
qa.find_differences(browsers=['Safari'],
                    users=['Public'],
                    item_types=['/biosamples/ENCBS632MTU/'])
```


Perform action before taking screenshot:
```python
qa.find_differences(browsers=['Safari'],
                    users=['Public'],
                    action_tuples=[('/experiments/ENCSR985KAT/', OpenUCSCGenomeBrowserHG19)])
```

Use item_type and click_path instead of action_tuple:
```python
qa.find_differences(browsers=['Safari'],
                    users=['Public'],
                    item_types=['/experiments/ENCSR985KAT/'],
                    click_paths=[OpenUCSCGenomeBrowserHG19])
```

#### More examples in [overview notebook](https://nbviewer.jupyter.org/github/ENCODE-DCC/pyencoded-tools/blob/master/jupyter_notebooks/qancode_overview.ipynb)

## Requires
* Python 3
* Selenium (https://pypi.python.org/pypi/selenium) (_pip install selenium_)
* OpenCV for Python 3 (_pip install opencv-python_)
* pandas (_pip install pandas_)
* PIL (_pip install Pillow_)
* tqdm (_pip install tqdm_)
* Chrome webdriver (https://sites.google.com/a/chromium.org/chromedriver/downloads)
* Firefox webdriver (https://github.com/mozilla/geckodriver/releases)
* OSX >10.12.5 and Safari Technology Preview Edition (https://developer.apple.com/safari/technology-preview) (bug in Safari 10.1.1 prevents Selenium script from running)
* Browser and Selenium driver downloads that currently work for _qancode_ (https://drive.google.com/drive/u/1/folders/0Bw5F9KFA-sq5aWx5Vlc3QUlfWmM)

## Additional documentation
* [How To: QA Automation](https://docs.google.com/document/d/1G1-TofLknZKq4FUVUjhqMKqjkH5sRhRS03tUXi-NKZU/edit?usp=sharing)

* [_qancode_ versus  manual  QA](https://docs.google.com/document/d/1e-sc79XioOq_D7TRn4EAo4jEfg7riWFwpjUNoVMZCSo/edit?usp=sharing)
