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
from datetime import datetime
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
        self.jobname = 'download'
        self.start_date, self.end_date = date_range
        self.main_path = main_path
        self.output_path = None
        self.log_path = None
        self.constructed = self._construct()

    def _construct(self):
        """before execution, check parameters are all in place"""
        # under main_path, create subfolder download-xxxx and log file inside subfolder
        time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        self.output_path = os.path.join(self.main_path, self.jobname+'-'+time_str)
        self.log_path = os.path.join(self.output_path, self.jobname+".log")
        if not os.path.isdir(self.main_path):
            os.makedirs(self.main_path)
        if not os.path.isdir(self.output_path):
            os.makedirs(self.output_path)
        createLogger(self.jobname, self.log_path)
        logger = logging.getLogger(self.jobname)
        try:
            self.start_date = str2intDate(self.start_date)
            self.end_date = str2intDate(self.end_date)
            assert self.start_date <= self.end_date, "Not a valid date range"
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True

    def run(self):
        """execution phase based on parameters"""
        urls = getDataAddress(self.start_date, self.end_date, "web", None, self.jobname)
        downloadData(urls, self.output_path, self.jobname)
        return self.output_path


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
                 config_path=None):
        self.jobname = 'clean'
        self.start_date, self.end_date = date_range
        self.input_path = input_path
        self.log_path = None
        self.output_type = output_type
        self.config_path = config_path
        self.constructed = self._construct()

    def _construct(self):
        """before execution, check parameters are all in place"""
        # under input_path, create log file
        time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        self.log_path = os.path.join(self.input_path, self.jobname+"-{}.log".format(time_str))
        createLogger(self.jobname, self.log_path)
        logger = logging.getLogger(self.jobname)
        try:
            self.start_date = str2intDate(self.start_date)
            self.end_date = str2intDate(self.end_date)
            assert self.start_date <= self.end_date, "Not a valid date range"
            assert self.output_type in ("sqlite","postgres"), "Output_type could only be sqlite, postgres"
            assert os.path.isfile(self.config_path), "config_path not valid"
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True

    def execute(self):
        """execution phase based on parameters"""
        assert self.constructed, "Please call construct method first."
        # step 1: get data location
        urls = getDataAddress(self.start_date, self.end_date,
                              self.input_format, self.input_path, None)
        # step 2: if web, download data, else pass
        data_paths = downloadData(urls, self.input_path, None)
        # step 3: if clean, clean data files, else pass
        if self.output_format == "text":
            return
        for dp in data_paths:
            # df = processData(dp)
            if self.output_format == "text":
                print(dp)
                #df.to_csv(self.output_path, index=False)


def getDataAddress(int_start_date, int_end_date, input_format, input_path, prefix):
    logger = logging.getLogger(prefix)
    """get data urls or paths"""
    urls = []
    data_regex = re.compile(r'^data.*?txt$')
    if input_format == "web":
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = [u['href'] for u in soup.find_all('a', href=data_regex)]
    elif input_format == 'local':
        urls = filter(data_regex.search, os.listdir(input_path))
    filter_urls = [u for u in urls if parseDate(u) >= int_start_date and parseDate(u) <= int_end_date]
    if not filter_urls:
        logger.error('No data files found within time window')
        sys.exit(1)
    logger.info('{} available data files within time window'.format(len(filter_urls)))
    return filter_urls


def downloadData(urls, output_path, prefix):
    """download data from mta web"""
    logger = logging.getLogger(prefix)
    i = 0
    for u in urls:
        filepath = os.path.join(output_path, u.split('/')[-1])
        if not os.path.exists(filepath):
            data = urlopen(URL + u).read()
            with open(filepath, 'wb+') as f:
                f.write(data)
            i += 1
            if i % 10 == 0:
                logger.info("Downloaded {} files...".format(i))
        else:
            logger.info("File exists: {}".format(filepath))
    logger.info("Complete: download {0} out of {1} files.".format(i, len(urls)))


def processData(dp, logger):
    """process downloaded data files
        a. check # columns
        b. combine date and time column, convert type to datetime
        c. convert entry/exit to integer type
        d. return pandas dataframe
    """
    datevalue = parseDate(dp)
    filename = os.path.split(dp)[-1]
    with open(dp, 'r') as f:
        data = f.read().split('\n')
    rows = []
    for i, line in enumerate(data):
        if datevalue >= 141018 and i == 0:
            # post 141018, data file has header, skip first row
            continue
        line = line.replace('\x00', '')
        cols = line.strip().split(',')
        ncol = len(cols)
        if ((datevalue < 141018 and (ncol - 3) % 5 > 0)
                or (datevalue >= 141018 and ncol != 11)):
            # check number of columns in each row
            logger.debug('File %s line %d: Incorrect number of items (%s)',
                         filename, i, ncol)
            continue
        if datevalue < 141018:
            # keys: ca/units/scp, every 5: daten/timen/descn/entriesn/exitsn
            for j in range(3, ncol, 5):
                row = processSingleRow(cols[:3], cols, filename, i, j, logger)
                if row:
                    rows.append(row)
                else:
                    continue
        else:
            # skip station name columns in the middle
            row = processSingleRow(cols[:3], cols, filename, i, 6, logger)
            if row:
                rows.append(row)
            else:
                continue
    labels = ['booth', 'remote', 'scp', 'datetime', 'description',
              'entries', 'exits']
    df = pd.DataFrame.from_records(rows, columns=labels)
    return df


def checkLineItem(datevalue, ncol, file, i):
    if (datevalue < 141018 and (ncol - 3) % 5 > 0) or (datevalue >= 141018 and ncol != 11):
        logging.debug('File {} line {}: Incorrect number of items ({})'.format(file, i, ncol))
        return True
    else:
        return False


def processDateTime(dt, file, i, j):
    try:
        dt_formatted = datetime.strptime(dt, '%m-%d-%y %H:%M:%S ').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            dt_formatted = datetime.strptime(dt, '%m/%d/%Y %H:%M:%S ').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            logging.debug('File {} line {} column {}: Incorrect datetime format ({})'.format(file, i, j, dt))
            return False
    return dt_formatted.strftime('%Y-%m-%d %H:%M:%S')


def processInt(val, file, i, j):
    try:
        val_int = int(val)
    except:
        logging.debug('File {} line {} column {}: Incorrect int format ({})'.format(file, i, j, val))
        return False
    return val_int


def processSingleRow(keys, cols, filename, i, j, logger):
    row = []
    dt = cols[j]+" "+cols[j+1]
    dt_formatted = processDateTime(dt, filename, i, j)
    if dt_formatted:
        row.append(dt_formatted)
    else:
        logger.debug('File %s line %d column %d: error datetime format (%s)',
                     filename, i, j, dt)
        return False
    row.append(cols[j+2])
    for k in [3, 4]:
        val_int = processInt(cols[k], filename, i, j+k)#, cols[j+k])
        if val_int:
            row.append(val_int)
        else:
            logger.debug('File %s line %d column %d: error int format (%s)',
                         filename, i, j, cols[k])
            return False
    return tuple(keys+row)


if __name__ == "__main__":
    download = Downloader(date_range=("2014-08-01", "2014-08-10"))
    data_paths = download.run()
    print("Example complete.")
