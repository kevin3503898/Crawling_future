import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pprint import pprint
import json
from threading import Thread
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def crawl(date):
    r = requests.get('https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&goDay=&doQuery=1&dateaddcnt=&queryDate={}%2F{}%2F{}&commodityId='.format(date.year, date.month, date.day))
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.text, 'html.parser')
    else:
        print('connection error')
    
    try:
        table = soup.find('table', class_='table_f')
        trs = table.find_all('tr')[3:]
    except AttributeError:
        print('There is no data at', date.strftime('%Y/%m/%d'))
        return
    
    data = {}
    for tr in trs:
        tds = tr.find_all('td')
        ths = tr.find_all('th')
        if len(ths) == 3:
            product = ths[1].text.strip()
            who = ths[2].text.strip()
            what = [td.text.strip() for td in tds]
            data[product] = {who: what}
        else:
            who = ths[0].text.strip()
            what = [td.text.strip() for td in tds]
            data[product].update({who: what})
        
    print(date, '\n', data)
    return date, data


def save_json(date, data, path):
    filename = os.path.join(path, 'futures_' + date.strftime('%Y%m%d') + '.json')
    with open(filename, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        print('saved file to', filename)


download_dir = 'futures'
os.makedirs(download_dir, exist_ok=True)
start = datetime.now()

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    today = datetime.today()
    date = today

    while True:
        future = executor.submit(crawl, date)
        futures.append(future)

        date = date - timedelta(days=1)
        if date <= today - timedelta(days=730):
            break

    for future in as_completed(futures):  
        if future.result():
            date, data = future.result()
            save_json(date, data, download_dir)
                
                
end = datetime.now()
print("執行時間：", end - start)

