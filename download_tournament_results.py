import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from functools import reduce
from multiprocessing import Pool
from requests_html import HTMLSession
import sys
import time

start = time.time()
year = sys.argv[1]

url = "https://www.pgatour.com/tournaments/schedule."+year+".html"
page = requests.get(url)
soup = BeautifulSoup(page.content,'html.parser')
schedule =[a['href'] for a in soup.find_all('a',href=True) \
           if 'past-results' in a['href'] and \
           'zurich-classic-of-new-orleans' not in a['href'] and
          'ryder-cup' not in a['href'] and
          'match-play' not in a['href']]

pga_url = "https://www.pgatour.com"

headers4 =  ['Player','Position', 'Round 1','Round 2','Round 3','Round 4',
            'Total Score','Official Money','FedEx Cup Points']
headers3 = headers4[:5] + headers4[6:]

metadata = []

for event in schedule:
    title = event.split('/')[-2]
    if 'http' in event:
        url = event
    else:
        url = pga_url+event
    print(url)

    session = HTMLSession()
    r = session.get(url)
    r.html.render()

    soup = BeautifulSoup(r.html.raw_html,'html.parser')
    
    leaderboard = [[s.get_text().strip() for s in p.find_all('td')] \
             for p in soup.find_all('table')[1].find_all('tr')][3:]
    
    nrounds = len(leaderboard[0]) - 5
    
    header = ['Player','Position'] + ['Round %d' % (i+1) for i in range(nrounds)] + \
             ['Total Score','Official Money','FedEx Cup Points']
    
    df = pd.DataFrame(leaderboard,columns=header)
        
    df.to_csv('tournaments/%s-%s.csv' % (title,year),index=False)

    m = dict()
    m['Title'] = soup.find_all('h2',{'class':'title'})[0].find_all('span',{'class':'row'})[0].get_text()
    m['File'] = title

    info = ''.join([x.text for x in soup.find_all('span',{'class':'header-row'})]).split('\n')
    
    for i in info:
        a = i.split(':')
        if len(a) > 1:
            if a[0] in m.keys():
                m[a[0]] = m[a[0]]+';'+a[1].strip()
            else:
                m[a[0]] = a[1].strip()

    metadata.append(m)
    
metadata = pd.DataFrame(metadata)
metadata.to_csv('tournaments/metadata_%s.csv' % year,index=False)

end = time.time()
dt = (end-start)/60
print('Done with %s. Elapsed time: %0.2f min' % (year,dt))