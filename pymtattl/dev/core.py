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
from sqlalchemy_declarative import create_all_table, Device, Turnstile
from sqlalchemy.orm import sessionmaker


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
        self.date_range = date_range
        self._start_date, self._end_date = None, None
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
            self._start_date = str2intDate(self.date_range[0])
            self._end_date = str2intDate(self.date_range[1])
            assert self._start_date <= self._end_date, "Not a valid date range"
            logger.info("Download data between {0} and {1}".format(self._start_date, self._end_date))
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
        self.urls = getDataAddress(self._start_date, self._end_date, self._jobname, None)
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
    def __init__(self, date_range=None,
                 input_path='./data/', output_type='sqlite',
                 dbstring='sqlite:///test_data.db'):
        self._jobname = 'clean'
        self._bulk_process = 10
        self.date_range = date_range
        self._start_date, self._end_date = None, None
        self.input_path = input_path
        self.output_type = output_type
        self.dbstring = dbstring
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
            if self.date_range:
                self._start_date = str2intDate(self.date_range[0])
                self._end_date = str2intDate(self.date_range[1])
                assert self._start_date <= self._end_date, "Not a valid date range"
                logger.info("Use data files between {0} and {1}".format(self._start_date, self._end_date))
            else:
                logger.info("No date range specified, use all data files in folder")
            assert self.output_type in ("sqlite","postgres"), "Output_type could only be sqlite, postgres"
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True
    
    def _configDB(self):
        """configure database connection, initialize tables (xxx) if not exist"""
        engine = create_all_table(self.dbstring)
        return engine

    def _processFile(self, file):
        """process each data file, return pandas dataframe
            a. check # columns
            b. combine date and time column, convert to timestamp
            c. convert entry/exit to integer type
        """
        logger = logging.getLogger(self._jobname)
        datevalue = parseDate(file)
        with open(os.path.join(self.input_path, file), 'r') as f:
            data = f.readlines()
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
                    logger.warning('File {0} line{1}: Incorrect number of columns ({2})'.format(datevalue, i, ncol))
                else:
                    # first 3: ca/units/scp, every 5: daten/timen/descn/entriesn/exitsn
                    for j in range(3, ncol, 5):
                        row = processRow(datevalue, cols, i, j, self._jobname)
                        if row:
                            rows.append(row)
            else:
                if ncol != 11:
                    logger.warning('File {0} line{1}: Incorrect number of columns ({2})'.format(datevalue, i, ncol))
                else:               
                    # skip column 3,4,5 (station, linename, division)
                    row = processRow(datevalue, cols, i, 6, self._jobname)
                    if row:
                        rows.append(row)
        labels = ['ca', 'unit', 'scp', 'timestamp', 'description', 'entry', 'exit']
        df = pd.DataFrame.from_records(rows, columns=labels)
        logger.info('Finish processing: File {0}'.format(datevalue))
        return df

    def run(self):
        """execution phase based on parameters"""
        # get data location
        files = getDataAddress(self._start_date, self._end_date, self._jobname, self.input_path)
        # configure database
        engine = self._configDB()
        Session = sessionmaker(bind=engine)
        session = Session()
        query_device = session.query(Device)
        df_device = data_frame(query_device, [c.name for c in Device.__table__.columns])
        # process files
        for f in files[:1]:
            df = self._processFile(f)
            device_pairs = df[['ca', 'unit', 'scp']].drop_duplicates()
            print(df.head(3))



def getDataAddress(start_date, end_date, prefix, input_path=None):
    """get data urls or paths"""
    logger = logging.getLogger(prefix)
    data_regex = re.compile(r'turnstile.*?txt$')
    if input_path:
        urls = filter(data_regex.search, os.listdir(input_path)) # local
    else:
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = [u['href'] for u in soup.find_all('a', href=data_regex)] # web
    if (prefix == 'clean') and (start_date is None) and (end_date is None):
        filter_urls = list(urls)
    else:
        filter_urls = [u for u in urls if parseDate(u) >= start_date and parseDate(u) <= end_date]
    if not filter_urls:
        logger.error('Complete: No data files found within time window.')
        sys.exit(1)
    logger.info('{} available data files within time window'.format(len(filter_urls)))
    return filter_urls


def processRow(filename, cols, i, j, prefix):
    """process single row, return False whenever encounter error"""
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
        logging.warning('File {0} line {1} column {2},{3}: Incorrect int format ({4},{5})'.format(filename, i, j+3, j+4, entry, exit))
        return False
    return tuple(cols[:3] + [timestamp, description, entry, exit])


def data_frame(query, columns):
    """http://danielweitzenfeld.github.io/passtheroc/blog/2014/10/12/datasci-sqlalchemy/
    Takes a sqlalchemy query and a list of columns, returns a dataframe.
    """
    def make_row(x):
        return dict([(c, getattr(x, c)) for c in columns])
    return pd.DataFrame([make_row(x) for x in query])


if __name__ == "__main__":
    #download = Downloader(date_range=("2014-08-01", "2014-08-10"))
    #data_paths = download.run()
    clean = Cleaner(input_path='./data/download-20181031141109')
    clean.run()
    print("Example complete.")

