from urllib.request import urlopen
from bs4 import BeautifulSoup
import re


def get_data_urls(start, end):
    """
        return data urls on mta turnstile page,
        filter down to given timeframe,
        start/end: 6-digit int, yymmdd, ie. 180504 (May,4th,2018)
    """
    URL = 'http://web.mta.info/developers/'
    soup = BeautifulSoup(urlopen(URL+'turnstile.html'), 'lxml')
    urls = soup.findAll('a', href=re.compile('^data.*?txt$'))
    urls_filtered = []
    for u in urls:
        h = u.get('href')
        try:
            date_part = int(url.split('_')[1].split('.')[0])
        except (IndexError, ValueError):
            print("Date part could not be identified within %s"%url)
            date_part = 0 #as 0 will never be >= any start time
        if (date_part >= start) and (date_part <= end):
            urls_filtered.append(URL+h)
    return urls_filtered


if __name__ == "__main__":
    urls = get_data_urls(start=140513,end=150513)
    print("Data url example: ", urls[0])
