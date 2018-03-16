from __future__ import absolute_import, print_function, unicode_literals

import os
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from utils import createFolder, writeDB, getPubDate, notEmptyStr, strParms
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


class DBDownloader(BaseDownloader):
    """
    Downlaod data and furthermore, save to SQLite database
    """

    def __init__(self, work_dir='Current', start=None, end=None,
                 dbtype='sqlite', dbparms={}):
        super().__init__(work_dir, start, end)
        assert dbtype == 'sqlite' or dbtype == 'postgres'
        self.dbtype = dbtype
        self.dbparms = dbparms
        self.new = False
        self.conn_string = ''
        self.local = True
        self.data_path = ''

    def auth_db(self, dbparms):
        if 'dbname' not in dbparms.keys() or notEmptyStr(dbparms['dbname']):
            create = input("No dbname provided. Do you want to create a new db? (y/n)").lower()
            if create == 'y':
                dbparms['dbname'] = input("Please enter a valid dbname:").lower()
                self.new = True
            else:
                raise ValueError("<dbname> required.")
        if self.dbtype == 'postgres':
            assert 'user' in dbparms.keys(), "<user> required in postgres parameters."
            assert 'password' in dbparms.keys(), "<password> required in postgres parameters."
            assert 'host' in dbparms.keys(), "<host> required in postgres parameters."
        # update parameters
        self.dbparms = dbparms
        return

    def build_conn(self, dbparms):
        dbname = dbparms['dbname']
        if self.dbtype == 'sqlite':
            if dbname.split('.')[-1] != 'db':
                dbname += '.db'
            self.conn_string = os.path.join(self.work_dir, dbname)
        elif self.dbtype == 'postgres':
            if self.new:
                pre_Parms = {k: self.dbparms[k] for k in self.dbparms.keys() and k != 'dbname'}
                self.conn_string = strParms(pre_Parms) + " dbname=postgres"
            else:
                self.conn_string = strParms(self.dbparms)
        return dbname

    def conn_db(self):
        if self.dbtype == 'sqlite':
            return sqlite3.connect(self.conn_string)
        elif self.dbtype == 'postgres':
            return psycopg2.connect(dsn=self.conn_string)

    def init_db(self):
        """
        Test connection and create db if not exists
        """
        self.auth_db(dbparms=self.dbparms)
        dbname = self.build_conn(dbparms=self.dbparms)

        if self.dbtype == 'sqlite':
            if os.path.exists(self.conn_string):
                print('Database {} exists.'.format(dbname))
            else:
                self.new = True
        try:
            con = self.conn_db()
            if self.dbtype == 'postgres':
                con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                c = con.curor()
                c.execute("SELECT 1 FROM pg_catalog.pg_database "
                            "WHERE datname='{}'".format(dbname))
                if c.fetchone()[0]:
                    print("Database {} exists.".format(dbname))
                elif self.new:
                    c.execute('CREATE DATABASE {};'.format(dbname))
                    self.conn_string = strParms(self.dbparms)
                    print("Created Database {}.".format(dbname))
                else:
                    raise ValueError("Database {} does NOT exist.".format(dbname))
        except Exception as e:
            print(e)
            raise
        finally:
            if con:
                con.close()
        return

    def init_tables(self):
        """
        Create or update tables if missing
        """
        con = self.conn_db()
        c = con.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS turnstiles '
                  '(booth text, remote text, scp text, date text, time text, '
                  'desc text, entries integer, exits integer)')
        c.execute('CREATE TABLE IF NOT EXISTS name_keys '
                  '(remote text, booth text, station text, '
                  'line text, devision text)')
        c.execute('CREATE TABLE IF NOT EXISTS file_names (file text)')
        con.commit()
        con.close()
        print('Created <turnstiles>, <name-keys>, <file_names> tables '
              'if not exist.')
        return

    def init_data(self, folder):
        data_path = createFolder(self.work_dir, folder)
        if os.path.isdir(data_path):
            urls = os.listdir(data_path)
            if sum(['turnstile' in u for u in urls]) > 1:
                return (data_path, urls)
            else:
                print("There is no turnstile data in the directory.")
        else:
            print("Couldn't find {}.".format(data_path))
        txt = input("Download text files first? If not will not keep a copy of text files. (y/n)").lower()
        if txt == 'y':
            if not notEmptyStr(folder):
                folder = input("Input data folder name:").lower()
            data_path = super().download_to_txt(data_folder=folder)
            urls = os.listdir(data_path)
            return (data_path, urls)
        elif txt == 'n':
            print("Download from urls, might take some time...")
            self.local = False
            data_path = self.work_dir
            urls = super().get_urls()
            return (data_path, urls)
        else:
            raise ValueError("Not valid input.")

    def download_to_db(self, data_folder='data'):
        """
        Download mta turnstile data and write to database
        """
        data_path, urls = self.init_data(data_folder)
        self.init_db()
        self.init_tables()

        con = self.conn_db()
        c = con.cursor()

        get_efiles = c.execute('SELECT file FROM file_names').fetchall()
        exist_files = [i[0] for i in get_efiles]

        i = 0
        for url in urls:
            fname = url.split('/')[-1]
            if fname not in exist_files and fname.startswith('turnstile'):
                if self.local:
                    url = os.path.join(data_path, url)
                    with open(url, 'r') as f:
                        data = f.read().split('\n')
                else:
                    data = urlopen(url).read().decode('utf-8').split('\n')
                try:
                    c = writeDB(fname, data, c)
                    c.execute('INSERT INTO file_names VALUES (?)', (fname,))
                    i += 1
                except Exception as e:
                    print(e)
                    raise
                finally:
                    if con:
                        con.close()


            elif fname not in exist_files and fname.endswith('xls'):
                if self.local:
                    url = os.path.join(data_path, url)
                try:
                    res_data = pd.read_excel(url, header=0)
                    res_data.columns = ['remote', 'booth', 'station',
                                        'line', 'division']
                    res_data.to_sql('name_keys', con=con, if_exists="replace",
                                    index=False)
                    c.execute('INSERT INTO file_names VALUES (?)', (fname,))
                    i += 1
                except Exception as e:
                    print(e)
                    raise
                finally:
                    if con:
                        con.close()


            if i > 0 and i % 5 == 0:
                con.commit()
                print("Adding {} files to the database...".format(i))
        con.commit()
        con.close()
        print("Finish writing {} files to the database.".format(i))
        return
