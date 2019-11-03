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
from datetime import datetime, timedelta
from .sqlalchemy_declarative import (Station, Device, Turnstile, Previous, 
                                    create_all_table, get_one_or_create, data_frame)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import INTEGER
from .utils import createLogger, createPath, str2intDate, parseDate, filterUrl

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
    def __init__(self, directory='./data/', local=True,
                 dbstring='sqlite:///mta_sample.db'):
        JOB = 'clean'
        self.dbstring = dbstring

        now = datetime.now().strftime("%Y%m%d%H%M%S")
        self.directory = directory
        self._input_path = createPath(os.path.join(directory, 'download'))
        log_dir = createPath(os.path.join(directory, 'log'))
        self._log_path = os.path.join(log_dir, now+".log")
        self.logger = createLogger(JOB, self._log_path)       

    def _create(self, date_range):
        """before execution, check parameters are all in place"""
        dateRange = [str2intDate(date_range[0]), str2intDate(date_range[1])]
        assert dateRange[0] <= dateRange[1], "Not a valid date range."
        self.logger.info("Use data files between {}.".format(dateRange))
        return dateRange

    def _retreive(self, date_range):
        """get all file paths"""
        data_regex = re.compile(r'turnstile_\d{6}.txt$')
        urls = list(filter(data_regex.search, os.listdir(self._input_path))) # local
        self.logger.info('{} data files found in local dir.'.format(len(urls)))
        filter_urls = filterUrl(urls, date_range)
        if not filter_urls:
            self.logger.error('Complete: No data file found within time window.')
            sys.exit(1)
        filter_urls.sort() # need to be sorted to make the decumulate step easier
        self.logger.info('{} available data files within time window.'.format(len(filter_urls)))
        return filter_urls
    
    def _configDB(self):
        """configure database connection, initialize tables (xxx) if not exist"""
        engine = create_all_table(self.dbstring)
        self.logger.info('Connected to dababase')
        return engine
    
    def _readFile(self, url):
        """read into data object given file path"""
        datevalue = parseDate(url)
        with open(os.path.join(self._input_path, url), 'r') as f:
            data = f.readlines()
        return data, datevalue

    def _process(self, data, datevalue):
        """process each data file, return pandas dataframe
            a. check # columns
            b. combine date and time column, convert to timestamp
            c. convert entry/exit to integer type
        """
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
                    self.logger.warning('File {0} line{1}: Incorrect number of columns ({2}).'.format(datevalue, i, ncol))
                else:
                    # first 3: ca/units/scp, every 5: daten/timen/descn/entriesn/exitsn
                    for j in range(3, ncol, 5):
                        row = processRow(self.logger, datevalue, cols, i, j)
                        if row:
                            rows.append(row)
            else:
                if ncol != 11:
                    self.logger.warning('File {0} line{1}: Incorrect number of columns ({2}).'.format(datevalue, i, ncol))
                else:
                    # skip column 3,4,5 (station, linename, division)
                    row = processRow(self.logger, datevalue, cols, i, 6)
                    if row:
                        rows.append(row)
        labels = ['ca', 'unit', 'scp', 'timestamp', 'description', 'entry', 'exit']
        df = pd.DataFrame.from_records(rows, columns=labels)
        df = df.sort_values(by=['ca', 'unit', 'scp', 'timestamp']).set_index(['ca', 'unit', 'scp'])
        self.logger.info('Finish processing: File {0}'.format(datevalue))
        return df

    def run(self, date_range=("2018-01-01", "2018-02-01")):
        """execution phase based on parameters"""
        # get data paths
        date_range = self._create(date_range)
        urls = self._retreive(date_range)

        # configure database
        engine = self._configDB()
        Session = sessionmaker(bind=engine)
        session = Session()

        # process files
        for u in urls:
            f, datevalue = self._readFile(u)
            df = self._process(f, datevalue)

            # insert new ca,unit,scp pair to db and index df
            for idx in df.index.unique():
                station, exist1 = get_one_or_create(session, Station, ca=idx[0], unit=idx[1])
                device, exist2 = get_one_or_create(session, Device, station_id=int(station.id), scp=idx[2])
                df.loc[idx, 'device_id'] = int(device.id)
            df = df.reset_index(drop=True) # keep only: device_id, timestamp description, entry, exit
            ## append historical step: 
            ## include last week last record per device_id to perform decumulate operation
            ## output last row for each device_id
            df_prev_new = df.groupby('device_id').last().reset_index()
            df_prev_new['file_date'] = datevalue # device_id, timestamp, description, entry, exit, file_date
            ## get stored last week records from db and convert to pandas dataframe
            df_prev = data_frame(session.query(Previous), [c.name for c in Previous.__table__.columns]) # id, device_id, description, timestamp, entry, exit, file_date
            self.logger.info("Sample of Previous table:\n{}".format(df_prev.head(5)))
            if not df_prev.empty:
                ## attach this week last records to part of df_prev, which will be the new df_prev for next week
                df_prev.drop(['id'], axis=1, inplace=True) # drop primary key
                df_rest = df_prev.loc[~df_prev['device_id'].isin(df['device_id']), :].copy()
                df_prev_new = pd.concat([df_prev_new, df_rest], axis=0, sort=False).reset_index(drop=True)
                ## select records 1) appeared in last week file 2) has device_id within current week file
                df_prev['file_date'] =  pd.to_datetime(df_prev['file_date'], format="%y%m%d")
                df_prev['last_week'] = ((df_prev['file_date'] - pd.to_datetime(datevalue, format="%y%m%d")).dt.days == -7)
                df_update = df_prev.loc[(df_prev['last_week']) & (df_prev['device_id'].isin(df['device_id'].unique())), :].copy()
                df_update.drop(['file_date', 'last_week'], axis=1, inplace=True)
                df = pd.concat([df, df_update], axis=0, sort=False)
            ## decumulate step:
            # 3 issues: backwards counts -> negative values,
            #           jump counts -> series of large numbers resulted from diff once,
            #           device reset -> huge values
            # a. use absolute values of diff (for backward counts)
            # b. remove first rows (with NAs) after diff
            # c. manual search for large number threshold (entry: 7000, exit: 6000), perform a second diff (for jump counts)
            # d. drop numbers above threshold (for huge values)
            df = df.sort_values(['device_id', 'timestamp']).reset_index(drop=True)
            df.loc[:, ['entry', 'exit']] = df.groupby('device_id')['entry', 'exit'].diff().abs()
            df.dropna(axis=0, how='any', inplace=True)
            df = splitDiff(df, 7000, 'entry', 'device_id')
            df = splitDiff(df, 6000, 'exit', 'device_id')
            df.dropna(axis=0, how='any', inplace=True)
            df.drop(df[(df['entry'] >= 7000) | (df['exit'] >= 6000)].index, inplace=True)
            logger.info("Sample of df:\n{}".format(df.head()))
            ## store decumulated data and new df_prev to db
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


def processRow(logger, filename, cols, i, j):
    """process single row, return False whenever encounter error"""
    timestamp = cols[j] + " " + cols[j+1]
    try:
        timestamp = datetime.strptime(timestamp, '%m-%d-%y %H:%M:%S')
    except ValueError:
        try:
            timestamp = datetime.strptime(timestamp, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            logger.warning('File {0} line {1} column {2}: Incorrect datetime format ({3}).'.format(filename, i, j, timestamp))
            return None
    # convert datetime to integer (seconds since epoch)
    timestamp = int((timestamp - datetime(1970, 1, 1)) / timedelta(seconds=1))
    description = cols[j+2]
    try:
        entry = int(cols[j+3])
        exit = int(cols[j+4])
    except TypeError:
        logging.warning('File {0} line {1} column {2},{3}: Incorrect int format ({4},{5}).'.format(filename, i, j+3, j+4, entry, exit))
        return None
    return tuple(cols[:3] + [timestamp, description, entry, exit])


def splitDiff(df, threshold, col, gbcol):
    """split df by threshold, perform a second groupby.diff on gte portion
        handle entry and exit seperately to save more "normal" records
        (data issue: every second record correct, every adjacent record wrong)
    """
    df1 = df[df[col] < threshold].copy()
    df2 = df[df[col] >= threshold].copy()
    df2[col] = df2.groupby(gbcol)[col].diff().abs()
    return pd.concat([df1, df2], axis=0).sort_index()
