import os
import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from . import utils
import pandas as pd


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


def read_prior(url):
    """
        Parse mta turnstile data prior to 2014-10-18
        drop scp
        return pandas dataframe
    """
    data = urlopen(url)
    rows = []
    err_count = 0
    for line in data:
        cols = line.strip().split(',')
        ncol = len(cols)
        if (ncol - 3) % 5 > 0:
            err_count += 1
        else:
            # keys: ca/units
            # + every 5: daten/timen/descn/entriesn/exitsn
            keys = cols[:2]
            for i in range(3, ncol, 5):
                row = tuple(keys+cols[i:i+5])
                rows.append(row)
    labels = ['booth', 'remote', 'date', 'time',
              'description', 'entries', 'exits']
    df = pd.DataFrame.from_records(rows, columns=labels)
    print("%d invalid lines with wrong # columns" % err_count)
    return df


def read_current(url):
    """
        Parse mta turnstile data current (>= 2014-10-18)
        drop scp, station, linename, division
        return pandas dataframe
    """
    labels = ['booth', 'remote', 'scp', 'station', 'linename', 'division',
              'date', 'time', 'description', 'entries', 'exits']
    use = [0, 1, 6, 7, 8, 9, 10]
    df = pd.read_table(url, sep=',', header=0, names=labels, usecols=use)
    return df


def load_tables(init=True, paths=None):
    id_dfcols = ['booth', 'remote']
    lt_dfcols = ['index_id', 'date_time', 'raw_entries', 'raw_exits']
    if isinstance(paths, list) and len(paths) == 2:
        index_df = pd.read_table(paths[0], sep=',', index_col=0,
                                 header=0, names=id_dfcols)
        latest_df = pd.read_table(paths[1], sep=',', names=lt_dfcols)
    elif init is True:
        index_df = pd.DataFrame(columns=id_dfcols)
        latest_df = pd.DataFrame(columns=lt_dfcols)
    else:
        raise Exception("Please provide init_tables")
    return (index_df, latest_df)


def read_urls(urls, init=True, paths=None):
    index, latest = load_tables(init, paths)
    for u in urls:
        if utils.getPubDate(u) < 141018:
            df = read_prior(u)
        else:
            df = read_current(u)
        print("Load into dataframe, start processing...")

        # convert to date_time and integer columns
        df['date'] = df['date'].apply(utils.formatDate)
        df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df.drop(['date', 'time'], axis=1, inplace=True)
        df['entries', 'exits'] = df['entries', 'exits'].applymap(int)
        # update index table, replace booth,remote with index_id
        idcols = ['booth', 'remote']
        id_pairs = df[idcols].drop_duplicates()
        index = index.append(id_pairs, ignore_index=True).drop_duplicates()
        df = df.merge(index.reset_index(), on=idcols, how='left')
        df.drop(idcols, axis=1, inplace=True)
    index.to_csv('index_table.txt', index_label='id')
    # question: do i really need scp level?


if __name__ == "__main__":
    cleaner = Cleaner()
    urls = cleaner.getUrls(start='2014-05-13',
                           end='2015-05-13')
    print("Data url example: ", urls[0])
