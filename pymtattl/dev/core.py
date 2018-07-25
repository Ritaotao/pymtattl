from __future__ import absolute_import, print_function, unicode_literals

import os
import re
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from . import utils
from . import load

now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
output_dir = "/data-{}/".format(now)


class Constructor:
    """Construction Phase
        Take in job requirements (not do anythign yet):
            date range, output type/format/path, input_path, config_path
        Examine database parameters if output_format postgresql
    """
    def __init__(self, date_range=("2010-03-05", "2010-03-05"),
                 output_type="raw", output_format="text",
                 output_path=output_dir, input_path=None, config_path="/"):
        self.start, self.end = date_range
        self.output_type = output_type
        self.output_format = output_format
        self.output_path = output_path
        self.input_path = input_path
        self.config_path = config_path
        self.check_settings()

    def check_settings(self):
        # check date
        self.start = int(datetime.strptime(self.start,
                                           '%Y-%m-%d').strftime('%y%m%d'))
        self.end = int(datetime.strptime(self.end,
                                         '%Y-%m-%d').strftime('%y%m%d'))
        assert self.start <= self.end, "Not a valid date range"
        # check output_type, output_format
        assert (self.output_type in ("raw", "clean"),
                "output_type could only be raw, clean")
        assert (self.output_foramt in ("text", "sqlite", "postgresql"),
                "output_format could only be text, sqlite, postgresql")
        # check output_path if text or sqlite
        if (self.output_format in ("text", "sqlite")
            and not os.path.isdir(self.output_path)):
                os.makedir(self.output_path)
        # check input_path
        if self.input_path is not None:
            assert os.path.isdir(input_path), "local_path not valid"
        # check config_path
        assert os.path.isdir(config_path), "config_path not valid"


class Executor(Constructor):
    """Execution Phase
        Download required data from mta turnstile data page
    """
    def __init__(self, *args, **kwargs):
        super(Executor, self).__init__(*args, **kwargs)
        self.urls = None

    def get_urls_or_paths(self):
        """get data urls or paths"""
        data_regex = re.compile(r'^data.*?txt$')
        if self.input_path is not None:
            URL = "http://web.mta.info/developers/"
            soup = BeautifulSoup(urlopen(URL+"turnstile.html"), "lxml")
            urls = soup.findAll('a', href=data_regex)
        else:
            file_paths = os.listdir(self.input_path)
            urls = filter(data_regex.search, file_paths)
        filter_urls = [u for u in urls if self.parse_date(u) >= self.start
                       and self.parse_date(u) <= self.end]
        if not filter_urls:
            raise Exception("No data files found within time window")
        else:
            self.urls = filter_urls


    def parse_date(self, name):
        """ Return date part (yymmdd) given data url or file name """
        return int(name.split('_')[-1].split('.')[0])



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
