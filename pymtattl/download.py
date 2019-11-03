"""
    ETL for New York turnstiles entries and exits data files from MTA webpage
    1. Download unprocessed txt data files
    2. Output decumulated, normalized data to database
"""

from __future__ import absolute_import, print_function, unicode_literals
import os
import sys
import re
from urllib.request import urlopen
from datetime import datetime
from bs4 import BeautifulSoup
from .utils import createLogger, createPath, str2intDate, parseDate, filterUrl

URL = "http://web.mta.info/developers/"

class Downloader:
    """
        Download Phase:
        Take in job requirements:
            date range: (start_date(str), end_date(str)),
            main_path (required, store data files): directory(str),
            verbose
    """

    def __init__(self, directory='./data/', local=True):
        JOB = 'download'

        now = datetime.now().strftime("%Y%m%d%H%M%S")
        self.directory = createPath(directory)
        self._output_path = createPath(os.path.join(directory, 'download'))
        log_dir = createPath(os.path.join(directory, 'log'))
        self._log_path = os.path.join(log_dir, now+".log")
        self.logger = createLogger(JOB, self._log_path)

    def _create(self, date_range):
        """before execution, check parameters are all in place"""
        dateRange = [str2intDate(date_range[0]), str2intDate(date_range[1])]
        assert dateRange[0] <= dateRange[1], "Not a valid date range."
        self.logger.info("Download data between {}.".format(dateRange))
        return dateRange

    def _retreive(self, date_range):
        """get all txt file urls from mta site"""
        data_regex = re.compile(r'turnstile_\d{6}.txt$')
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = [u['href'] for u in soup.find_all('a', href=data_regex)]
        self.logger.info('{} data file urls found on mta site.'.format(len(urls)))
        filter_urls = filterUrl(urls, date_range)
        if not filter_urls:
            self.logger.error('Complete: No data file found within time window.')
            sys.exit(1)
        self.logger.info('{} available data files within time window.'.format(len(filter_urls)))
        return filter_urls

    def _download(self, urls, verbose):
        """download data from mta web"""
        self.logger.info("Start downloading process.")
        paths = []
        for i, u in enumerate(urls):
            p = os.path.join(self._output_path, u.split('/')[-1])
            if not os.path.exists(p):
                data = urlopen(URL + u).read()
                with open(p, 'wb+') as f:
                    f.write(data)
                paths.append(p)
            else:
                self.logger.info("File exists: {}".format(p))
            if (i > 0) and (i % verbose == 0):
                self.logger.info("Processed {} files...".format(i))
        self.logger.info("Complete: downloaded {0} out of {1} files.".format(len(paths), len(urls)))
        return paths

    def run(self, date_range=("2018-01-01", "2018-02-01"), verbose=10):
        """execution phase based on parameters"""
        try:
            date_range = self._create(date_range)
            urls = self._retreive(date_range)
            paths = self._download(urls, verbose)
            return paths
        except Exception as e:
            self.logger.error(e, exc_info=True)