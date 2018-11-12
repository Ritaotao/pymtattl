"""
    ETL for New York turnstiles entries and exits data files from MTA webpage
    1. Download unprocessed txt files to local (input_path)
    2. (If output_type = None) End.
       (If output_type = "text") tabular, cleaned csv files (not merged).
       (If output_type = "sqlite"/"postgres") normalize at booth, remote level;
            find last_row_table to compute diff of cumulative sum.
"""

from __future__ import absolute_import, print_function, unicode_literals
import os
import sys
import re
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging

URL = "http://web.mta.info/developers/"
LOGFORMAT = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGFORMAT)


def createLogger(prefix, log_path=None):
    """create logging instance"""
    appLogger = logging.getLogger(prefix)
    appLogger.addHandler(logging.NullHandler())
    # write log to file
    if log_path:
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(LOGFORMAT))
        appLogger.addHandler(fh)


def str2intDate(datetext):
    """take string date format input and convert to numerics"""
    return int(datetime.strptime(datetext, '%Y-%m-%d').strftime('%y%m%d'))


def parseDate(name):
    """return date part (yymmdd) given data url or file name"""
    return int(name.split('_')[-1].split('.')[0])


def setPath(main_path, prefix='download'):
    """return output path and logfile path"""
    time_str = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = os.path.join(main_path, prefix+'-'+time_str)
    log_path = os.path.join(output_path, prefix + ".log")
    return output_path, log_path


class Downloader:
    """
        Download Phase:
        Take in job requirements:
            date range: (start_date(str), end_date(str)),
            main_path (required, store data files): directory(str),
    """
    def __init__(self, date_range=('2010-05-05', '2010-05-15'),
                 main_path='./data/'):
        self._jobname = 'download'
        self.start_date, self.end_date = date_range
        self.main_path = main_path
        self._output_path = None
        self._log_path = None
        self._constructed = self._construct()
        self._urls = []

    def _construct(self):
        """before execution, check parameters are all in place"""
        # under main_path, create subfolder download-xxxx and log file inside subfolder
        time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        self._output_path = os.path.join(self.main_path, self._jobname+'-'+time_str)
        self._log_path = os.path.join(self._output_path, self._jobname+".log")
        if not os.path.isdir(self.main_path):
            os.makedirs(self.main_path)
        if not os.path.isdir(self._output_path):
            os.makedirs(self._output_path)
        createLogger(self._jobname, self._log_path)
        logger = logging.getLogger(self._jobname)
        try:
            self.start_date = str2intDate(self.start_date)
            self.end_date = str2intDate(self.end_date)
            assert self.start_date <= self.end_date, "Not a valid date range"
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True

    def _download(self):
        """download data from mta web"""
        logger = logging.getLogger(self._jobname)
        i = 0
        for u in self._urls:
            filepath = os.path.join(self._output_path, u.split('/')[-1])
            if not os.path.exists(filepath):
                data = urlopen(URL + u).read()
                with open(filepath, 'wb+') as f:
                    f.write(data)
                i += 1
                if i % 10 == 0:
                    logger.info("Downloaded {} files...".format(i))
            else:
                logger.info("File exists: {}".format(filepath))
        logger.info("Complete: download {0} out of {1} files.".format(i, len(self._urls)))
        return

    def run(self):
        """execution phase based on parameters"""
        self.urls = getDataAddress(self.start_date, self.end_date, self._jobname, None)
        self._download()
        return self._output_path


class Cleaner:
    """
        Clean Phase:
        Take in job requirements:
            date range: (start_date(str), end_date(str)),
            input_path (required, read raw text files): directory(str),
            output_type: "sqlite"|"postgres",
            config_path (json file path including database connection parameters): dir(str)
    """
    def __init__(self, date_range=("2010-05-05", "2010-05-15"),
                 input_path='./data/', output_type='sqlite',
                 dbparam=None):
        self._jobname = 'clean'
        self._bulk_process = 10
        self.start_date, self.end_date = date_range
        self.input_path = input_path
        self.output_type = output_type
        self.dbparam = None
        self._log_path = None
        self._constructed = self._construct()

    def _construct(self):
        """before execution, check parameters are all in place"""
        # under input_path, create log file
        time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        self._log_path = os.path.join(self.input_path, self._jobname+"-{}.log".format(time_str))
        createLogger(self._jobname, self._log_path)
        logger = logging.getLogger(self._jobname)
        try:
            self.start_date = str2intDate(self.start_date)
            self.end_date = str2intDate(self.end_date)
            assert self.start_date <= self.end_date, "Not a valid date range"
            assert self.output_type in ("sqlite","postgres"), "Output_type could only be sqlite, postgres"
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True

    def run(self):
        """execution phase based on parameters"""
        # get data location
        files = getDataAddress(self.start_date, self.end_date, self._jobname, self.input_path)
        # configure database
        ### placeholder
        


def getDataAddress(int_start_date, int_end_date, prefix, input_path=None):
    logger = logging.getLogger(prefix)
    """get data urls or paths"""
    urls = []
    data_regex = re.compile(r'^data.*?txt$')
    if input_path:
        urls = filter(data_regex.search, os.listdir(input_path)) # local
    else:
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = [u['href'] for u in soup.find_all('a', href=data_regex)] # web
    filter_urls = [u for u in urls if parseDate(u) >= int_start_date and parseDate(u) <= int_end_date]
    if not filter_urls:
        logger.error('No data files found within time window')
        sys.exit(1)
    logger.info('{} available data files within time window'.format(len(filter_urls)))
    return filter_urls


def processData(file, prefix):
    """process downloaded data files
        a. check # columns
        b. combine date and time column, convert type to datetime
        c. convert entry/exit to integer type
        d. return pandas dataframe
    """
    logger = logging.getLogger(prefix)
    datevalue = parseDate(file)
    filename = os.path.split(file)[-1]
    with open(file, 'rb') as f:
        data = f.read().split('\n')
    rows = []
    for i, line in enumerate(data):
        # post 141018, data file has header, skip first row
        if datevalue >= 141018 and i == 0:   
            continue
        line = line.replace('\x00', '')
        cols = line.strip().split(',')
        ncol = len(cols)
        if datevalue < 141018:
            if (ncol - 3) % 5 > 0:
                logger.warning('File {0} line{1}: Incorrect number of columns ({3})'.format(filename, i, ncol))
                continue               
            # first 3: ca/units/scp, every 5: daten/timen/descn/entriesn/exitsn
            for j in range(3, ncol, 5):
                row = processRow(filename, cols, i, j, prefix)
                if row:
                    rows.append(row)
                else:
                    continue
        else:
            if ncol != 11:
                logger.warning('File {0} line{1}: Incorrect number of columns ({3})'.format(filename, i, ncol))
                continue                 
            # skip column 3,4,5 of station, linename, division
            row = processRow(filename, cols, i, 6, logger)
            if row:
                rows.append(row)
            else:
                continue
    labels = ['ca', 'unit', 'scp', 'timestamp', 'description', 'entry', 'exit']
    df = pd.DataFrame.from_records(rows, columns=labels)
    return df


def processRow(filename, cols, i, j, prefix):
    logger = logging.getLogger(prefix)
    timestamp = cols[j] + " " + cols[j+1]
    try:
        timestamp = datetime.strptime(timestamp, '%m-%d-%y %H:%M:%S')
    except ValueError:
        try:
            timestamp = datetime.strptime(timestamp, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            logger.warning('File {0} line {1} column {2}: Incorrect datetime format ({3})'.format(filename, i, j, timestamp))
            return False
    timestamp = int((timestamp - datetime(1970, 1, 1)) / timedelta(seconds=1))
    description = cols[j+2]
    try:
        entry = int(cols[j+3])
        exit = int(cols[j+4])
    except TypeError:
        logging.debug('File {0} line {1} column {2},{3}: Incorrect int format ({4},{5})'.format(filename, i, j+3, j+4, entry, exit))
        return False
    return tuple(cols[:3] + [description, entry, exit])


if __name__ == "__main__":
    download = Downloader(date_range=("2014-08-01", "2014-08-10"))
    data_paths = download.run()
    print("Example complete.")
