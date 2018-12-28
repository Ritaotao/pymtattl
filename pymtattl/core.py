"""
    ETL for New York turnstiles entries and exits data files from MTA webpage
    1. Download unprocessed txt data files
    2. Output decumulated, normalized data to database
"""

from __future__ import absolute_import, print_function, unicode_literals
import os
import sys
import re
import logging
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime, timedelta
from .sqlalchemy_declarative import (Station, Device, Turnstile, Previous, 
                                    create_all_table, get_one_or_create, data_frame)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import INTEGER


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


class Downloader:
    """
        Download Phase:
        Take in job requirements:
            date range: (start_date(str), end_date(str)),
            main_path (required, store data files): directory(str),
            verbose
    """
    def __init__(self, date_range=("2018-01-01", "2018-02-01"),
                 main_path='./data/', verbose=10):
        self._jobname = 'download'
        self.date_range = date_range
        self._start_date, self._end_date = None, None
        self.main_path = main_path
        self.verbose = int(verbose)
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
            assert self._start_date <= self._end_date, "Not a valid date range."
            logger.info("Download data between {0} and {1}.".format(self._start_date, self._end_date))
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True

    def _download(self):
        """download data from mta web"""
        logger = logging.getLogger(self._jobname)
        logger.info("Start downloading process.")
        i = 0
        for u in self._urls:
            filepath = os.path.join(self._output_path, u.split('/')[-1])
            if not os.path.exists(filepath):
                data = urlopen(URL + u).read()
                with open(filepath, 'wb+') as f:
                    f.write(data)
                i += 1
                if (self.verbose != 0) and (i % self.verbose == 0):
                    logger.info("Downloaded {} files...".format(i))
            else:
                logger.info("File exists: {}".format(filepath))
        logger.info("Complete: downloaded {0} out of {1} files.".format(i, len(self._urls)))
        return

    def run(self):
        """execution phase based on parameters"""
        self._urls = getDataAddress(self._start_date, self._end_date, self._jobname, None)
        self._download()
        return self._output_path


class Cleaner:
    """
        Clean Phase:
        Take in job requirements:
            date range: (start_date(str), end_date(str)),
            input_path (required, read raw text files): directory(str),
            dbstring: database urls used by sqlalchemy(str);
                dialect+driver://username:password@host:port/database
                postgres: 'postgresql://scott:tiger@localhost/mydatabase'
                mysql: 'mysql://scott:tiger@localhost/foo'
                sqlite: 'sqlite:///foo.db'
                (more info could be found here: https://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql)
    """
    def __init__(self, date_range=None,
                 input_path='./data/',
                 dbstring='sqlite:///mta_sample.db'):
        self._jobname = 'clean'
        self.date_range = date_range
        self._start_date, self._end_date = None, None
        self.input_path = input_path
        self.dbstring = dbstring
        self._log_path = None
        self._constructed = self._construct()

    def _construct(self):
        """before execution, check parameters are all in place"""
        # create log file under input path
        time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        self._log_path = os.path.join(self.input_path, self._jobname+"-{}.log".format(time_str))
        createLogger(self._jobname, self._log_path)
        logger = logging.getLogger(self._jobname)
        try:
            if self.date_range:
                self._start_date = str2intDate(self.date_range[0])
                self._end_date = str2intDate(self.date_range[1])
                assert self._start_date <= self._end_date, "Not a valid date range."
                logger.info("Use data files between {0} and {1}.".format(self._start_date, self._end_date))
            else:
                logger.info("No date range specified, use all data files in folder.")
        except Exception as e:     
            logger.error(e, exc_info=True)
            raise
        return True
    
    def _configDB(self):
        """configure database connection, initialize tables (xxx) if not exist"""
        logger = logging.getLogger(self._jobname)
        try:
            engine = create_all_table(self.dbstring)
            logger.info('Connected to dababase')
            return engine
        except Exception as e:
            logger.error('Failed to connect to database.\n{}'.format(e))
            sys.exit(1)

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
                    logger.warning('File {0} line{1}: Incorrect number of columns ({2}).'.format(datevalue, i, ncol))
                else:
                    # first 3: ca/units/scp, every 5: daten/timen/descn/entriesn/exitsn
                    for j in range(3, ncol, 5):
                        row = processRow(datevalue, cols, i, j, self._jobname)
                        if row:
                            rows.append(row)
            else:
                if ncol != 11:
                    logger.warning('File {0} line{1}: Incorrect number of columns ({2}).'.format(datevalue, i, ncol))
                else:               
                    # skip column 3,4,5 (station, linename, division)
                    row = processRow(datevalue, cols, i, 6, self._jobname)
                    if row:
                        rows.append(row)
        labels = ['ca', 'unit', 'scp', 'timestamp', 'description', 'entry', 'exit']
        df = pd.DataFrame.from_records(rows, columns=labels)
        df = df.sort_values(by=['ca', 'unit', 'scp', 'timestamp']).set_index(['ca', 'unit', 'scp'])
        logger.info('Finish processing: File {0}'.format(datevalue))
        return df

    def run(self):
        """execution phase based on parameters"""
        # get data location
        logger = logging.getLogger(self._jobname)
        files = getDataAddress(self._start_date, self._end_date, self._jobname, self.input_path)
        # configure database
        engine = self._configDB()
        Session = sessionmaker(bind=engine)
        session = Session()
        # process files
        for f in files:
            df = self._processFile(f)
            int_file_date = parseDate(f)
            # insert new ca,unit,scp pair to db and index df
            for idx in df.index.unique():
                station, exist1 = get_one_or_create(session, Station, ca=idx[0], unit=idx[1])
                device, exist2 = get_one_or_create(session, Device, station_id=int(station.id), scp=idx[2])
                df.loc[idx, 'device_id'] = int(device.id)
            df = df.reset_index(drop=True) # drop index: ca,unit,scp, keep: device_id, timestamp description, entry, exit
            # diff step: include last week last record per device_id to perform decumulate operation across weekly files
            ## output last row for each device_id
            df_prev_new = df.groupby('device_id').last().reset_index()
            df_prev_new['file_date'] = int_file_date # device_id, timestamp, description, entry, exit, file_date
            ## get entire last_df from db
            df_prev = data_frame(session.query(Previous), [c.name for c in Previous.__table__.columns]) # id, device_id, description, timestamp, entry, exit, file_date
            logger.info("Sample of Previous table:\n{}".format(df_prev.head(5)))
            if not df_prev.empty:
                ## attach this week last records to part of df_prev, which will be the new df_prev for next week
                df_prev.drop(['id'], axis=1, inplace=True) # drop primary key
                df_rest = df_prev.loc[~df_prev['device_id'].isin(df['device_id']), :].copy()
                df_prev_new = pd.concat([df_prev_new, df_rest], axis=0, sort=False).reset_index(drop=True)
                ## convert file_date column to datetime since date subtraction is not valid in integer type across years
                ## select records 1) appeared in last week file 2) has device_id within current week file
                df_prev['file_date'] =  pd.to_datetime(df_prev['file_date'], format="%y%m%d")
                df_prev['last_week'] = ((df_prev['file_date'] - pd.to_datetime(int_file_date, format="%y%m%d")).dt.days == -7)
                df_update = df_prev.loc[(df_prev['last_week']) & (df_prev['device_id'].isin(df['device_id'].unique())), :].copy()
                df_update.drop(['file_date', 'last_week'], axis=1, inplace=True)
                df = pd.concat([df, df_update], axis=0, sort=False)
            ## decumulate step
            df = df.sort_values(['device_id', 'timestamp']).reset_index(drop=True)
            df['entry'] = df.groupby('device_id')['entry'].diff()
            df['exit'] = df.groupby('device_id')['exit'].diff()
            # might have negative values due to reasons ie. counter reset, etc, set to zero
            df.loc[df['entry'] < 0, 'entry'] = 0
            df.loc[df['exit'] < 0, 'exit'] = 0
            logger.info("Sample of df:\n{}".format(df.head()))
            # remove first rows (with NAs) after diff
            df.dropna(axis=0, how='any', inplace=True)    
            ## check out decumulated data and new df_prev
            df = df.to_dict(orient='records')
            try:
                session.execute(
                    Turnstile.__table__.insert(),
                    df
                )
                session.commit()
                logger.info("Complete: new week turnstile data inserted into table.")
                df_prev_new.index.names = ['id']
                df_prev_new.to_sql('previous', con=engine, if_exists='replace',dtype={'device_id': INTEGER()})
                logger.info("Complete: table Previous updated.")
            except Exception as e:
                logging.error("Unexpected exception happened while write to db, error detail:\n {}".format(e))
                session.rollback()
        session.close()


def getDataAddress(start_date, end_date, prefix, input_path=None):
    """get data urls or paths"""
    logger = logging.getLogger(prefix)
    data_regex = re.compile(r'turnstile_\d{6}.txt$')
    if input_path:
        # if cleaning step
        urls = list(filter(data_regex.search, os.listdir(input_path))) # local
        logger.info('{} data files found in local dir.'.format(len(urls)))
    else:
        # if downloading step
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = [u['href'] for u in soup.find_all('a', href=data_regex)] # web
        logger.info('{} data file urls found on mta site.'.format(len(urls)))
    if (prefix == 'clean') and (start_date is None) and (end_date is None):
        filter_urls = urls # during cleaning, when start/end date not specified, reading all files inside folder
    else:
        filter_urls = [u for u in urls if parseDate(u) >= start_date and parseDate(u) <= end_date]
    if not filter_urls:
        logger.error('Complete: No data file found within time window.')
        sys.exit(1)
    else:
        filter_urls.sort() # need to be sorted to make the decumulate step easier
        logger.info('{} available data files within time window.'.format(len(filter_urls)))
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
            logger.warning('File {0} line {1} column {2}: Incorrect datetime format ({3}).'.format(filename, i, j, timestamp))
            return False
    # convert datetime to integer (seconds since epoch)
    timestamp = int((timestamp - datetime(1970, 1, 1)) / timedelta(seconds=1))
    description = cols[j+2]
    try:
        entry = int(cols[j+3])
        exit = int(cols[j+4])
    except TypeError:
        logging.warning('File {0} line {1} column {2},{3}: Incorrect int format ({4},{5}).'.format(filename, i, j+3, j+4, entry, exit))
        return False
    return tuple(cols[:3] + [timestamp, description, entry, exit])

