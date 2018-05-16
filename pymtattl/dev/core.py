import os
import re
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from . import utils
from . import load


class Cleaner:
    """
        download, process, and save mta turnstile data page
        start/end: date string, yyyy-mm-dd, ie. 2018-05-04 (May,4th,2018)
    """
    def __init__(self, start, end):
        self.start = int(datetime.strptime(start,
                                           '%Y-%m-%d').strftime('%y%m%d'))
        self.end = int(datetime.strptime(end,
                                         '%Y-%m-%d').strftime('%y%m%d'))
        assert self.start < self.end, "End should not be earlier than start"
        self.urls = []

    def get_data_urls(self):
        """
            return data urls on mta turnstile page,
            filter down to given timeframe,
        """
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
            print("%d available data files within time window" % n)
        self.urls = urls_filtered
        return urls_filtered

    def get_raw_txt(self, path=os.getcwd()):
        assert self.urls, "Run get_data_urls first"
        for i, u in enumerate(self.urls):
            filepath = os.path.join(path, u.split('/')[-1])
            if not os.path.exists(filepath):
                data = urlopen(u).read()
                with open(filepath, 'w+') as f:
                    f.write(data)
                if i % 9 == 0:
                    print("Downloaded %d files..." % (i+1))
        print("Downloading completed.")
        return

    def to_txt(self, path=os.getcwd()):
        pass


if __name__ == "__main__":
    cleaner = Cleaner()
    urls = cleaner.getUrls(start='2014-05-13',
                           end='2015-05-13')
    print("Data url example: ", urls[0])
