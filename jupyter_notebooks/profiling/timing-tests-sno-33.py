
# coding: utf-8

# In[1]:



# In[2]:

import sys
sys.path.append('..')
import qancode
import numpy as np
import pandas as pd


# In[3]:

#compare any two encoded instances here
qa = qancode.QANCODE(rc_url="https://test.encodedcc.org", prod_url='https://sno68-on-dev.demo.encodedcc.org/')


# In[4]:

res = qa.check_response_time(item_types=["/","/search/?type=Experiment"], n=50)


# In[5]:

res


# In[6]:

qa.check_response_time(item_types=["/search/?searchTerm=K562", "/search/?searchTerm=DNA+binding", "/search/?searchTerm=human", "/search/?searchTerm=H3K36me3", "/search/?searchTerm=CTCF"], n=50)


# In[7]:

qa.check_response_time(item_types=['/ENCSR255XZG/', '/ENCSR749ILN/', '/ENCSR688GVV/', '/ENCSR301HAG/','/ENCSR315NAC/', '/ENCSR856JJB/'], n=50)


# In[8]:

qa.check_response_time(item_types=['/experiments/ENCSR255XZG/', '/experiments/ENCSR749ILN/', '/experiments/ENCSR688GVV/', '/experiments/ENCSR301HAG/','/experiments/ENCSR315NAC/', '/experiments/ENCSR856JJB/'], n=50)


# In[10]:

qa.check_response_time(json=True, item_types=['/ENCSR255XZG/', '/ENCSR749ILN/', '/ENCSR688GVV/', '/ENCSR301HAG/','/ENCSR315NAC/', '/ENCSR856JJB/'], n=50)


# In[ ]:

qa.check_response_time(item_types=['/ENCSR480OHP/','/ENCSR000EMB/','/ENCSR323QIP/','/ENCSR718AXQ/','/ENCSR165BGV/','/ENCSR384KAN/','/ENCSR330FXL/','/ENCSR825UNV/'], n=50)


# In[ ]:

qa.check_response_time(item_types=['/ENCAB830JLB/','/ENCAB000BKR/','/ENCAB284TTY/','/ENCAB294YUD/','/ENCAB000AOC/','/ENCAB301QZF/','/ENCAB000ANM/','/ENCAB000ANU/','/ENCAB445KMF/','/ENCAB000BAY/'], n=50)


# In[ ]:

qa.check_response_time(item_types=['/ENCBS787TFV/','/ENCBS676QAV/','/ENCBS996IWU/','/ENCBS913XJP/','/ENCBS565NTN/','/ENCBS896YZO/','/ENCBS280NYD/'], n=50)


# ## 
