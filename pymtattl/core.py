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
            # + every 5 add: daten/timen/entriesn/exitsn
            keys = cols[:2]
            for i in range(3, ncol, 5):
                row = tuple(keys+cols[i:i+2]+cols[i+3:i+5])
                rows.append(row)
    labels = ['booth', 'remote', 'date', 'time', 'entries', 'exits']
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
    use = [0, 1, 6, 7, 9, 10]
    df = pd.read_table(url, sep=',', header=0, names=labels, usecols=use)
    return df


def load_tables(init_tables=True):
    """
        Load or init index table and latest table,
        index table (primary key for data table):
            id, booth, remote;
        latest table (as base numbers for following data file):
            index_id, latest_time, raw_entries, raw_exits;
        init_tables: boolean or list,
            list of index table and latest table paths,
            default to true, create init_tables
    """
    id_dfcols = ['booth', 'remote']
    lt_dfcols = ['index_id', 'latest_time', 'raw_entries', 'raw_exits']
    if isinstance(init_tables, list) and len(init_tables) == 2:
        index_df = pd.read_table(init_tables[0], sep=',', index_col=0,
                                 header=0, names=id_dfcols)
        latest_df = pd.read_table(init_tables[1], sep=',', names=lt_dfcols)
    elif init_tables is True:
        index_df = pd.DataFrame(columns=id_dfcols)
        latest_df = pd.DataFrame(columns=lt_dfcols)
    else:
        raise Exception("Please provide init_tables")
    return (index_df, latest_df)


def update_index(df, index_table):
    """
        update index table with new booth, remote pairs,
        drop booth, remote columns, keep only index column from df
    """
    id_pairs = df[['booth', 'remote']].drop_duplicates()
    index = index_table.append(id_pairs, ignore_index=True).drop_duplicates()
    df = df.merge(index.reset_index(), on=['booth', 'remote'], how='left')
    df.drop(['booth', 'remote'], axis=1, inplace=True)
    df.rename(columns={'index': 'index_id'}, inplace=True)
    return (df, index_table)


def update_latest(df, latest_table):
    """
        update latest table with most recent raw numbers found in df
        for each index (booth, remote pairs)
        !!!pause: what if counter reset next week???
    """
    latest = df.loc[df.groupby('index_id')['date_time'].idxmax()]
    df = df.merge(latest_table, on='index_id', how='left')
    df[['raw_entries', 'raw_exits']].fillna(0, inplace=True)


def read_url(url, index_table, latest_table):
    if utils.getPubDate(url) < 141018:
        df = read_prior(url)
    else:
        df = read_current(url)
    print("Load into dataframe, start processing...")

    # convert to date_time and integer columns
    df['date'] = df['date'].apply(utils.formatDate)
    df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df.drop(['date', 'time'], axis=1, inplace=True)
    df['entries', 'exits'] = df['entries', 'exits'].applymap(int)

    df, index_df = update_index(df, index_table)
    df = df.groupby(['index_id', 'date_time'])['entries',
                                               'exits'].sum().reset_index()


if __name__ == "__main__":
    cleaner = Cleaner()
    urls = cleaner.getUrls(start='2014-05-13',
                           end='2015-05-13')
    print("Data url example: ", urls[0])
