# This allows you to run:
# $ python -i qancode
# >>> qa = QANCODE()
# >>> qa.compare_downloads(action_tuples=[('/ENCXXX', DownloadBEDFileFromModal)])
# Which is less typing than:
# >>> import qancode
# >>> qa = qancode.QANCODE()
# >>> qa.compare_downloads(action_tuples=[('/ENCXXX', qancode.DownloadBEDFileFromModal)])
import os
import sys
try:
    sys.path.remove('qancode')
except ValueError:
    pass
sys.path.append(os.path.abspath('.'))
from qancode import QANCODE
# Expose all click_paths to namespace.
from qancode.clickpaths import *
