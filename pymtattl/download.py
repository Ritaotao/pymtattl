from __future__ import absolute_import, print_function, unicode_literals

import os
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from utils import createFolder, writeDB, getPubDate, notEmptyStr
import sqlite3
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class BaseDownloader:
    """
    Download mta turnstile data page
    """
    MAIN_URL = 'http://web.mta.info/developers/'
    TURNSTILE_URL = 'http://web.mta.info/developers/turnstile.html'

    def __init__(self, work_dir='Current',
                 start=None, end=None):
        # define a valid folder for download data
        self.work_dir = createFolder(work_dir, branch=None)
        assert (start is None or isinstance(start, int)), \
            'Start should be None or 6-digit integer (yymmdd)'
        assert (end is None or isinstance(end, int)), \
            'End should be None or 6-digit integer (yymmdd)'
        self.start = start
        self.end = end

    def get_urls(self, keep_urls=False):
        """
        Get and save data urls
        """
        soup = BeautifulSoup(urlopen(self.__class__.TURNSTILE_URL), 'lxml')
        urls = soup.findAll('a', href=True)
        # data file urls
        dat_urls = [self.__class__.MAIN_URL + u.get('href')
                    for u in urls
                    if u.get('href').startswith('data')]
        if self.start is not None:
            dat_urls = [u for u in dat_urls
                        if getPubDate(u) >= self.start]
        if self.end is not None:
            dat_urls = [u for u in dat_urls
                        if getPubDate(u) <= self.end]
        if not dat_urls:
            raise ValueError('Can not identify any data urls '
                             'within specified timeframe, '
                             'please check start and end date.')

        # resource file urls (headers, lookup tables for station names)
        res_urls = [self.__class__.MAIN_URL + u.get('href')
                    for u in urls
                    if u.get('href').startswith('resources')]

        print("{} data files and {} resource files available for download."
              .format(len(dat_urls), len(res_urls)))

        urls = dat_urls + res_urls
        if keep_urls:
            file_path = os.path.join(self.work_dir, 'data_urls.txt')
            with open(file_path, 'w+') as f:
                f.write('\n'.join(urls))
            print("Write data_urls.txt to {}".format(self.work_dir))
        return urls

    def download_to_txt(self, data_folder='data'):
        """
        Download text files
        """
        dat_dir = createFolder(self.work_dir, data_folder)
        urls = self.get_urls()

        i = 0
        for url in urls:
            fpath = os.path.join(dat_dir, url.split('/')[-1])
            if not os.path.exists(fpath):
                try:
                    data = urlopen(url).read()
                except HTTPError as err:
                    print(err.code, err.reason)
                with open(fpath, 'wb+') as f:
                    f.write(data)
                i += 1
                if i % 10 == 0:
                    print("Wrote {} files...".format(i))
        print("Finish adding {} text files to {}. (# of Files in Dir: {})"
              .format(i, dat_dir, len(os.listdir(dat_dir))))
        return dat_dir


class SQLiteDownloader(BaseDownloader):
    """
    Downlaod and data and furthermore, save to database
    """

    def __init__(self, work_dir='Current', start=None, end=None,
                 dbname='data.db'):
        super().__init__(work_dir, start, end)
        self.dbname = dbname
        self.dbpath = ''

    def auth_db(self):
        assert notEmptyStr(self.dbname), 'Please use a valid db name.'
        if self.dbname.split('.')[-1] != 'db':
            self.dbname += '.db'
        self.dbpath = os.path.join(self.work_dir, self.dbname)
        return

    def init_db(self):
        """
        Test connection and create db if not exists
        """
        self.auth_db()
        if os.path.exists(self.dbpath):
            print('Database file {} already exists. '
                  'Update instead of create.'.format(self.dbname))
        try:
            conn = sqlite3.connect(self.dbpath)
            conn.close()
        except Exception as e:
            print(e)
            raise
        return

    def init_tables(self):
        """
        Create or update tables if missing
        """
        conn = sqlite3.connect(self.dbpath)
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS turnstiles '
                  '(booth text, remote text, scp text, date text, time text, '
                  'desc text, entries integer, exits integer)')
        c.execute('CREATE TABLE IF NOT EXISTS name_keys '
                  '(remote text, booth text, station text, '
                  'line text, devision text)')
        c.execute('CREATE TABLE IF NOT EXISTS file_names (file text)')
        conn.commit()
        conn.close()
        print('Created <turnstiles>, <name-keys>, <file_names> tables '
              'if not exist.')
        return

    def download_to_db(self, data_path=''):
        """
        Download data and store in a sqlite database
        """
        local = True
        try:
            urls = os.listdir(data_path)
            assert sum(['turnstile' in u for u in urls]) > 1
        except FileNotFoundError as e:
            print("Couldn't find local data folder.")
            txt = input("Download text files first? (y/n)").lower()
            if txt == 'y':
                folder = input("Input data folder name:").lower()
                data_path = super().download_to_txt(data_folder=folder)
                urls = os.listdir(data_path)
            elif txt == 'n':
                print("Download from urls, might take some time...")
                urls = super().get_urls()
                local = False
            else:
                print("Not valid input.")
                return

        self.init_db()
        self.init_tables()
        conn = sqlite3.connect(self.dbpath)
        c = conn.cursor()

        get_efiles = c.execute('SELECT file FROM file_names').fetchall()
        exist_files = [i[0] for i in get_efiles]

        i = 0
        for url in urls:
            fname = url.split('/')[-1]
            if fname not in exist_files and fname.startswith('turnstile'):
                if local:
                    with open(os.path.join(data_path, url), 'r') as f:
                        data = f.read().split('\n')
                else:
                    data = urlopen(url).read().decode('utf-8').split('\n')
                try:
                    c = writeDB(fname, data, c)
                except Exception as e:
                    conn.close()
                    print(e)
                    raise
                c.execute('INSERT INTO file_names VALUES (?)', (fname,))
                i += 1

            elif fname not in exist_files and fname.endswith('xls'):
                if local:
                    url = os.path.join(data_path, url)
                try:
                    res_data = pd.read_excel(url, header=0)
                    res_data.columns = ['remote', 'booth', 'station',
                                        'line', 'division']
                    res_data.to_sql('name_keys', con=conn, if_exists="replace",
                                    index=False)
                except Exception as e:
                    conn.close()
                    print(e)
                    raise
                c.execute('INSERT INTO file_names VALUES (?)', (fname,))
                i += 1

            if i > 0 and i % 5 == 0:
                conn.commit()
                print("Adding {} files to the database...".format(i))
        conn.commit()
        conn.close()
        print("Finish writing {} files to the database.".format(i))
        return self.dbpath


class PostgresDownloader(BaseDownloader):
    """
    Downlaod and data and furthermore, save to database
    """

    def __init__(self, work_dir, start=None, end=None, dbtype='sqlite'):
        super().__init__(work_dir, start, end)
        self.dbtype = dbtype
        self.dbname = ''
        self.dbparms = ''

    def auth_db(self, dbname='', user='', password='', host='', port=''):
        self.dbname = ifEmptyStr(dbname, 'Please use a valid db name.')
        user = ifEmptyStr(user, 'Please use a valid user name.')
        password = ifEmptyStr(password, 'Please use a valid password.')
        conn_string = "{}={} {}={}".format("user", user,
                                           "password", password)
        if notEmptyStr(host):
            conn_string += " {}={}".format("host", host)
        if notEmptyStr(port):
            conn_string += " {}={}".format("port", port)
        self.dbparms = conn_string
        return

    def create_posgresdb(self):
        try:
            conn_string = self.dbparms + " dbname=postgre"
            conn = psycopg2.connect(dsn=conn_string)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.curor()
            cur.execute("SELECT 1 FROM pg_catalog.pg_database "
                        "WHERE datname='{}'".format(self.dbname))
            if cur.fetchone()[0]:
                print("DB {} already exists.".format(self.dbname))
            else:
                cur.execute('CREATE DATABASE %s ;' % self.dbname)
            cur.close()
            con.close()
        except Exception as e:
            print(e)
            raise
        return
