from __future__ import absolute_import, print_function, unicode_literals

import os
import re
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import logging

#now = datetime.now().strftime("%Y%m%d%H%M%S")
#output_dir = "./data-clean-{}/".format(now)
#download_dir = "./data-download-{}/".format(now)
#log = "./app-{}.log".format(now)
#logging.basicConfig(filename=log)
def setLoggingPath(main_path, time_str, prefix='download'):
    logname = prefix + "-{}.log".format(time_str)
    log_path = os.path.join(main_path, logname)
    logging.basicConfig(filename=log_path)
URL = "http://web.mta.info/developers/"


def str2intDate(datetext):
    return int(datetime.strptime(datetext, '%Y-%m-%d').strftime('%y%m%d'))


def parseDate(name):
    """return date part (yymmdd) given data url or file name"""
    return int(name.split('_')[-1].split('.')[0])


class Download:
    """
        Download Phase:
        Take in job requirements (not do anything yet):
            date range: (start_date(str), end_date(str)),
            main_path (required, store data files): directory(str),
            config_path (default current working directory): directory(str)
    """
    def __init__(self, date_range=("2010-05-05", "2010-05-15"),
                 main_path='./data/'):
        self.start_date, self.end_date = date_range
        self.main_path = main_path
        self.output_path = None
        self.constructed = self.construct()

    def construct(self):
        """before execution, check parameters are all in place"""
        now_str = datetime.now().strftime("%Y%m%d%H%M%S")
        self.start_date = str2intDate(self.start_date)
        self.end_date = str2intDate(self.end_date)
        assert self.start_date <= self.end_date, "Not a valid date range"
        if not os.path.isdir(self.main_path):
            os.makedirs(self.main_path)
        self.output_path = os.path.join(self.main_path, "download-{}".format(now_str))
        if not os.path.isdir(self.output_path):
            os.makedirs(self.output_path)
        setLoggingPath(self.main_path, now_str, 'download')
        return True

    def run(self):
        """execution phase based on parameters"""
        assert self.constructed, "Construct method to verify params should be called first."
        # get data location
        urls = getDataAddress(self.start_date, self.end_date, "web", None)
        data_paths = downloadData(urls, self.output_path)
        return data_paths


class Clean:
    """
        Download Phase:
        Take in job requirements (not do anything yet):
            date range: (start_date(str), end_date(str)),
            input_format: "local"|"web",
            input_path (required, read/store raw text files): directory(str),
            output_type: "raw"|"clean",
            output_format: "text"|"sqlite"|"postgres",
            output_path (required, save clean text or sqlite files): directory(str),
            config_path (default current working directory): directory(str)
        Examine database parameters if output_format postgres
    """
    def __init__(self, date_range=("2010-05-05", "2010-05-15"),
                 input_format="web", input_path='a',
                 output_type="raw",
                 output_format="text", output_path='b',
                 config_path=os.getcwd()):
        self.start_date, self.end_date = date_range
        self.input_format = input_format
        self.input_path = input_path
        self.output_type = output_type
        self.output_format = output_format
        self.output_path = output_path
        self.config_path = config_path
        self.constructed = False

    def construct(self):
        """before execution, check parameters are all in place"""
        self.start_date = str2intDate(self.start_date)
        self.end_date = str2intDate(self.end_date)
        assert self.start_date <= self.end_date, "Not a valid date range"
        assert self.input_format in ("local", "web"), "input_format could only be local, web"
        if not os.path.isdir(self.input_path):
            os.makedirs(self.input_path)
        assert self.output_type in ("raw", "clean"), "output_type could only be raw, clean"
        assert self.output_format in ("text", "sqlite", "postgres"), "output_format could only be text, sqlite, postgres"
        if (self.output_format in ("text", "sqlite")
            and not os.path.isdir(self.output_path)):
                os.makedirs(self.output_path)
        assert os.path.isdir(self.config_path), "config_path not valid"

    def execute(self):
        """execution phase based on parameters"""
        assert self.constructed, "Please call construct method first."
        # step 1: get data location
        urls = getDataAddress(self.start_date, self.end_date,
                              self.input_format, self.input_path)
        # step 2: if web, download data, else pass
        data_paths = downloadData(urls, self.input_path)
        # step 3: if clean, clean data files, else pass
        if self.output_type == "raw" and self.output_format == "text":
            return
        for dp in data_paths:
            df = processData(dp)


def getDataAddress(int_start_date, int_end_date, input_format, input_path):
    """get data urls or paths"""
    urls = []
    data_regex = re.compile(r'^data.*?txt$')
    if input_format == "web":
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = [u['href'] for u in soup.find_all('a', href=data_regex)]
    elif input_format == 'local':
        file_paths = os.listdir(input_path)
        urls = filter(data_regex.search, file_paths)
    filter_urls = [u for u in urls if parseDate(u) >= int_start_date and parseDate(u) <= int_end_date]
    if not filter_urls:
        raise Exception("No data files found within time window")
    print("%d available data files within time window" % len(filter_urls))
    return filter_urls


def downloadData(urls, output_path):
    """download data from mta web"""
    i = 0
    data_paths = []
    for u in urls:
        filepath = os.path.join(output_path, u.split('/')[-1])
        if not os.path.exists(filepath):
            data = urlopen(URL + u).read()
            with open(filepath, 'wb+') as f:
                f.write(data)
            i += 1
            if i % 10 == 0:
                print("Downloaded %d files..." % i)
        else:
            print("File exists:", filepath)
        data_paths.append(filepath)
    print("Complete: download %d out of %d files." % (i, len(urls)))
    return data_paths


def processData(dp):
    datevalue = parseDate(dp)
    filename = os.path.split(dp)[-1]
    with open(dp, 'r') as f:
        data = f.read().split('\n')
    rows = []
    for i, line in enumerate(data):
        if datevalue >= 141018 and i == 0:
            continue
        line = line.replace('\x00', '')
        cols = line.strip().split(',')
        ncol = len(cols)
        if checkLineItem(datevalue, ncol, filename, i):
            continue
        if datevalue < 141018:
            # keys: ca/units/scp, every 5 add: daten/timen/descn/entriesn/exitsn
            for j in range(3, ncol, 5):
                row = processSingleRow(cols[:3], cols, filename, i, j)
                if row:
                    rows.append(row)
                else:
                    continue
        else:
            row = processSingleRow(cols[:3], cols, filename, i, 6)
            if row:
                rows.append(row)
            else:
                continue
    labels = ['booth', 'remote', 'scp', 'datetime', 'description', 'entries', 'exits']
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
    except ValueError as e:
        try:
            dt_formatted = datetime.strptime(dt, '%m/%d/%Y %H:%M:%S ').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logging.debug('File {} line {} column {}: Incorrect datetime format ({})'.format(file, i, j, dt))
            return False
    return dt_formatted


def processInt(val, file, i, j):
    try:
        val_int = int(val)
    except:
        logging.debug('File {} line {} column {}: Incorrect int format ({})'.format(file, i, j, val))
        return False
    return val_int


def processSingleRow(keys, cols, filename, i, j):
    row = []
    dt_formatted = processDateTime(cols[j]+" "+cols[j+1], filename, i, j)
    if dt_formatted:
        row.append(dt_formatted)
    else:
        return False
    row.append(cols[j+2])
    for k in [3, 4]:
        val_int = processInt(cols[k], filename, i, j+k)#, cols[j+k])
        if val_int:
            row.append(val_int)
        else:
            return False
    return tuple(keys+row)


if __name__ == "__main__":
    download = Download(date_range=("2014-08-01", "2014-12-31"))
    data_paths = download.run()
    print("Example complete.")
