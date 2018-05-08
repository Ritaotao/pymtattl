import os
import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from . import utils

class Cleaner:
    """
        download, process, and save mta turnstile data page
        start/end: date string, yyyy-mm-dd, ie. 2018-05-04 (May,4th,2018)
    """
    def __init__(self, start, end):
        self.start = None
        self.end = None
        self.urls = []

    def get_data_urls(self, start, end):
        """
            return data urls on mta turnstile page,
            filter down to given timeframe,
        """
        self.start = int(datetime.strptime(start,'%Y-%m-%d').strftime('%y%m%d'))
        self.end = int(datetime.strptime(end,'%Y-%m-%d').strftime('%y%m%d'))
        assert self.start < self.end, "End should not be earlier than start"

        URL = 'http://web.mta.info/developers/'
        soup = BeautifulSoup(urlopen(URL+'turnstile.html'), 'lxml')
        urls = soup.findAll('a', href=re.compile('^data.*?txt$'))
        urls_filtered = []
        for u in urls:
            h = u.get('href')
            date_int = utils.getPubDate(h)
            if (date_int >= self.start) and (date_int <= self.end):
                urls_filtered.append(URL+h)
        n = len(urls_filtered)
        if n == 0:
            raise Exception("No data files found within time window")
        else:
            print("%d data files found within time window"%n)
        self.urls = urls_filtered
        return urls_filtered

    def process(self):
        assert self.urls, "Run get_data_urls first"
        for u in self.urls:
            if utils.getPubDate(u) < 141018:
                df = utils.clean_prior(u)
            else:
                df = utils.clean_current(u)


    def to_txt(self, path=os.getcwd()):


def read_prior(url):
    """
        Parse mta turnstile data prior to 2014-10-18
        return pandas dataframe
    """
    data = urlopen(url)
    rows = []
    err_count = 0
    for line in data:
        cols = line.strip().split(',')
        ncol = len(cols)
        if (ncol - 3) % 5 > 0:
            err_count += 1
        else:
            # keys: ca/units/scp
            # + every 5: daten/timen/descn/entriesn/exitsn
            keys = cols[:3]
            for i in range(3, ncol, 5):
                row = tuple(keys+cols[i:i+5])
                rows.append(row)
    labels = ['booth','remote','scp','date','time','description',
              'entries','exits']
    df = pd.DataFrame.from_records(rows, columns=labels)
    print("%d invalid lines with wrong # columns"%err_count)
    return df


def read_current(url):
    """
        Parse mta turnstile data current (>= 2014-10-18)
        return pandas dataframe
    """
    labels = ['booth','remote','scp','station','linename','division',
              'date','time','description','entries','exits']
    use = [0,1,2,6,7,8,9,10]
    df = pd.read_table(url, sep=',', skiprows=1, header=None,
                       names=labels, usecols=use)
    return df


def read_urls(urls):
    for u in urls:
        if getPubDate(u) < 141018:
            df = read_prior(u)
        else:
            df = read_current(u)
        df['date'] = df['date'].apply(formatDate)
        df['entries','exits'] = df['entries','exits'].applymap(int)


if __name__ == "__main__":
    cleaner = Cleaner()
    urls = cleaner.getUrls(start='2014-05-13',
                           end='2015-05-13')
    print("Data url example: ", urls[0])
