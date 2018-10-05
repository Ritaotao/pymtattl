from __future__ import absolute_import, print_function, unicode_literals

import os
import re
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import logging

now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
output_dir = "/data-clean-{}/".format(now)
input_dir = "/data-raw-{}/".format(now)
log = "/app-{}.log".format(now)
logging.basicConfig(filename=log)
URL = "http://web.mta.info/developers/"



def str2intDate(datetext):
    return int(datetime.strptime(datetext, '%Y-%m-%d').strftime('%y%m%d'))


def parseDate(name):
    """return date part (yymmdd) given data url or file name"""
    return int(name.split('_')[-1].split('.')[0])


class Job:
    """
        Construction Phase:
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
                 input_format="web", input_path=input_dir,
                 output_type="raw",
                 output_format="text", output_path=output_dir,
                 config_path=os.getcwd()):
        self.start, self.end = date_range
        self.input_format = input_format
        self.input_path = input_path
        self.output_type = output_type
        self.output_format = output_format
        self.output_path = output_path
        self.config_path = config_path
        self.constructed = False

    def construct(self):
        """before execution, check parameters are all in place"""
        self.start = str2intDate(self.start)
        self.end = str2intDate(self.end)
        assert self.start <= self.end, "Not a valid date range"
        assert (self.input_format in ("local", "web"),
                "input_format could only be local, web")
        if not os.path.isdir(self.input_path):
            os.makedir(self.input_path)
        assert (self.output_type in ("raw", "clean"),
                "output_type could only be raw, clean")
        assert (self.output_format in ("text", "sqlite", "postgres"),
                "output_format could only be text, sqlite, postgres")
        if (self.output_format in ("text", "sqlite")
            and not os.path.isdir(self.output_path)):
                os.makedir(self.output_path)
        assert os.path.isdir(config_path), "config_path not valid"

    def execute(self):
        """execution phase based on parameters"""
        assert self.constructed, "Please call construct method first."
        # step 1: get data location
        urls = getDataLocation(self.input_format, self.input_path,
                               self.start_date, self.end_date)
        # step 2: if web, download data, else pass
        data_paths = downloadData(self.input_format, self.input_path, urls)
        # step 3: if clean, clean data files, else pass
        if output_type == "raw" and output_format == "text":
            return
        for dp in data_paths:
            df = processData(dp)


def getDataLocation(input_format, input_path, start_date, end_date):
    """get data urls or paths"""
    data_regex = re.compile(r'^data.*?txt$')
    if input_format == "web":
        soup = BeautifulSoup(urlopen(URL + "turnstile.html"), "lxml")
        urls = soup.findAll('a', href=data_regex)
    else:
        file_paths = os.listdir(input_path)
        urls = filter(data_regex.search, file_paths)
    filter_urls = [u for u in urls if parseDate(u) >= start_date
                   and parseDate(u) <= end_date]
    if not filter_urls:
        raise Exception("No data files found within time window")
    print("%d available data files within time window" % len(filter_urls))
    return filter_urls


def downloadData(input_format, input_path, urls):
    """download data from mta web"""
    if input_format == "web":
        data_paths = []
        for i, u in enumerate(urls):
            filepath = os.path.join(input_path, u.split('/')[-1])
            if not os.path.exists(filepath):
                data = urlopen(u).read()
                with open(filepath, 'w+') as f:
                    f.write(data)
                if i % 9 == 0:
                    print("Downloaded %d files..." % (i+1))
            data_paths.append(filepath)
        print("Downloading completed.")
        return data_paths
    else:
        print("Use local raw data directory.")
        return urls


def processData(dp):
    datevalue = parseDate(dp)
    filename = os.path.split(dp)[-1]
    with open(dp, 'r') as f:
        data = f.read().split('\n'))
    rows = []
    for i, line in enumerate(data):
        if datevalue >= 141018 and i = 0:
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
        logging.debug('File %s line %d: Incorrect number of items (%s)'.format(file, i, ncol))
        return True
    else:
        return False


def processDateTime(dt, file, i, j):
    try:
        dt_formatted = datetime.strptime(dt, '%m-%d-%y %H:%M:%S ').strftime('%Y-%m-%d %H:%M:%S'))
    except ValueError as e:
        try:
            dt_formatted = datetime.strptime(dt, '%m/%d/%Y %H:%M:%S ').strftime('%Y-%m-%d %H:%M:%S'))
        except ValueError as e:
            logging.debug('File %s line %d column %d: Incorrect datetime format (%s)'.format(file, i, j, dt))
            return False
    return dt_formatted


def processInt(val, file, i, j):
    try:
        val_int = int(val)
    except:
        logging.debug('File %s line %d column %d: Incorrect int format (%s)'.format(file, i, j, val))
        return False
    return val_int


def processSingleRow(keys, cols, filename, i, j):
    row = []
    dt_formatted = processDatetime(cols[j]+" "+cols[j+1], filename, i, j)
    if dt_formatted:
        row.append(dt_formatted)
    else:
        return False
    row.append(cols[j+2])
    for k in [3, 4]:
        val_int = processInt(cols[k], filename, i, j+k, cols[j+k])
        if val_int:
            row.append(val_int)
        else:
            return False
    return tuple(keys+row)


if __name__ == "__main__":
    cleaner = Cleaner()
    urls = cleaner.getUrls(start='2014-05-13',
                           end='2015-05-13')
    print("Data url example: ", urls[0])
