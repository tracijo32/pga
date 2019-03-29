
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from functools import reduce
from multiprocessing import Pool
import sys

# In[2]:


## grab statistic categories from main stats page
def get_categories(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    categories = [a['href'] for a in soup.find_all('a',href=True) if re.match('(/stats/categories.\w*.html)',a['href']) is not None]
    return categories


# In[3]:


## grab dictionary of stat name and corresponding url link to table from a page
def get_stat(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    html = list(soup.children)[3]
    stats = {a.get_text():a['href'] for a in html.find_all('a',href=True) if re.match('(/stats/stat.\d+.html)',a['href']) is not None}
    return stats


# In[4]:


## get a dataframe of the table on the webpage at url
def make_table(url,stat,year):
    if year < 2019:
        url_list = url.split('.')
        url_list.insert(-1,str(year))
        url = '.'.join(url_list)
    page = ''
    while page == '':
        try:
            page = requests.get(url)
            break
        except:
            print("Retry accessing "+url)
            time.sleep(0.5)
            continue
    page = requests.get(url)
            
    soup = BeautifulSoup(page.content,'html.parser')
    headers = [h.get_text().split(' ')[0].capitalize() for h in soup.find_all('table')[1].find_all('th')]

    stats = [[s.get_text().strip() for s in p.find_all('td')] for p in soup.find_all('table')[1].find_all('tr')][1:]
    df = pd.DataFrame(stats,columns=headers)
    df['Year'] = year
    df['Statistic'] = stat
    
    headers.remove('Rank')
    headers.remove('Rank')
    headers.remove('Player')
    df_melt = pd.melt(df,id_vars=['Player','Year','Statistic'],value_vars=headers,
                      var_name='Variable',value_name='Value')
    return df_melt


# In[5]:


pga_url = "https://www.pgatour.com"


# In[6]:


categories = get_categories(pga_url+'/stats.html')


# In[7]:


stats = reduce(lambda a, b: dict(a, **b), [get_stat(pga_url+c) for c in categories])
stats = {s:u for s,u in stats.items() if ('02685' not in u) and ('02445' not in u)}


# In[11]:

y = int(sys.argv[1])

start = time.time()
pool = Pool(processes=8)
results = [pool.apply_async(make_table,args=(pga_url+u,s,y)) for s,u in stats.items()]
output = [p.get() for p in results]
data = pd.concat(output)
end = time.time()
dt = (end-start)/60
df = pd.concat(output)
df.to_csv("player_stats/player_stats_%d.csv" % y,index=False)
print("Done with %d. Elapsed time: %0.2f min" % (y,dt))

