from __future__ import absolute_import, print_function, unicode_literals

import os
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from .utils import (createFolder, writeDB, getPubDate,
                    notEmptyStr, strParms, dbInsert, dbPK)
import sqlite3
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine


class BaseDownloader:
    """
    Download mta turnstile data page
    """
    MAIN_URL = 'http://web.mta.info/developers/'
    TURNSTILE_URL = 'http://web.mta.info/developers/turnstile.html'

    def __init__(self, start=None, end=None):
        # define a valid folder for download data
        assert (start is None or isinstance(start, int)), \
            'Start should be None or 6-digit integer (yymmdd)'
        assert (end is None or isinstance(end, int)), \
            'End should be None or 6-digit integer (yymmdd)'
        self.start = start
        self.end = end

    def get_urls(self, path='', keep_urls=False):
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
            path = createFolder(path, branch=None)
            file_path = os.path.join(path, 'data_urls.txt')
            if os.path.exists(file_path):
                os.remove(file_path)
                print('Original data_urls.txt remvoed.')
            with open(file_path, 'w+') as f:
                f.write('\n'.join(urls))
            print("Write data_urls.txt to {}".format(path))
        return urls

    def download_to_txt(self, path='', keep_urls=True):
        """
        Download text files
        """
        dat_dir = createFolder(path, branch=None)
        urls = self.get_urls(path=dat_dir, keep_urls=keep_urls)

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
    Downlaod mta turnstile data and save to database
    """

    def __init__(self, start=None, end=None,
                 dbtype='sqlite', dbparms={}):
        super().__init__(start, end)
        assert dbtype == 'sqlite' or dbtype == 'postgres'
        self.dbtype = dbtype
        self.dbparms = dbparms
        self.new = False
        self.conn_string = ''
        self.local = True

    def auth_db(self, dbparms):
        """
        Authenticate db arguments
        """
        if not('dbname' in dbparms.keys() and notEmptyStr(dbparms['dbname'])):
            create = input("No dbname provided. "
                           "Do you want to create a new db? (y/n)").lower()
            if create == 'y':
                dbparms['dbname'] = input("Please enter a valid dbname:")
                self.new = True
            else:
                raise ValueError("<dbname> required.")
        if self.dbtype == 'postgres':
            assert 'user' in dbparms.keys(), ("<user> required "
                                              "in postgres parameters.")
            assert 'password' in dbparms.keys(), ("<password> required "
                                                  "in postgres parameters.")
            assert 'host' in dbparms.keys(), ("<host> required "
                                              "in postgres parameters.")
            assert 'port' in dbparms.keys(), ("<port> required "
                                              "in postgres parameters.")
        elif self.dbtype == 'sqlite':
            assert 'path' in dbparms.keys(), ("<path> required "
                                              "in sqlite parameters.")
            dbparms['path'] = createFolder(dbparms['path'])
        return dbparms

    def build_conn(self, dbparms):
        """
        Form connection argument for db
        """
        dbname = dbparms['dbname']
        if self.dbtype == 'sqlite':
            if dbname.split('.')[-1] != 'db':
                dbname += '.db'
            self.conn_string = os.path.join(dbparms['path'], dbname)
        elif self.dbtype == 'postgres':
            if self.new:
                pre_Parms = {k: ('postgres' if k == 'dbname' else dbparms[k])
                             for k, v in dbparms.items()}
                self.conn_string = strParms(pre_Parms)
            else:
                self.conn_string = strParms(dbparms)
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
        dbparms = self.auth_db(self.dbparms)
        dbname = self.build_conn(dbparms)

        if self.dbtype == 'sqlite':
            if os.path.exists(self.conn_string):
                print('Database {} exists.'.format(dbname))
            else:
                self.new = True
        try:
            con = self.conn_db()
            if self.dbtype == 'postgres':
                if self.new:
                    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    c = con.cursor()
                    c.execute("SELECT 1 FROM pg_catalog.pg_database "
                              "WHERE datname='{}';".format(dbname))
                    if c.fetchone():
                        print("Database {} exists.".format(dbname))
                    else:
                        c.execute('CREATE DATABASE {};'.format(dbname))
                        self.conn_string = strParms(dbparms)
                        print("Created Database {}.".format(dbname))
            con.close()
        except Exception as e:
            print(e)
            raise

    def init_tables(self):
        """
        Create or update tables if missing
        """
        con = self.conn_db()
        c = con.cursor()
        pk = dbPK(self.dbtype)
        tn = ("CREATE TABLE IF NOT EXISTS turnstiles (" + pk +
              ", booth TEXT, remote TEXT, scp TEXT, date TEXT, "
              "time TEXT, description TEXT, entries INTEGER, "
              "exits INTEGER);")
        nk = ("CREATE TABLE IF NOT EXISTS name_keys (" + pk +
              ", remote TEXT, booth TEXT, station TEXT, "
              "line TEXT, devision TEXT);")
        fn = ("CREATE TABLE IF NOT EXISTS file_names (" + pk +
              ", file TEXT);")
        c.execute(tn)
        c.execute(nk)
        c.execute(fn)
        con.commit()
        con.close()
        print('Created <turnstiles>, <name_keys>, <file_names> tables '
              'if not exist.')
        return

    def init_data(self, data_path='data', update=False):
        """
        Moniter and acquire online and local data files
        """
        if os.path.isdir(data_path):
            if update:
                data_path = super().download_to_txt(path=data_path,
                                                    keep_urls=True)
            urls = os.listdir(data_path)
            dat_urls = [u for u in urls if u.startswith('turnstile')]
            res_urls = [u for u in urls if u.endswith('xls')]
            if self.start is not None:
                dat_urls = [u for u in dat_urls
                            if getPubDate(u) >= self.start]
            if self.end is not None:
                dat_urls = [u for u in dat_urls
                            if getPubDate(u) <= self.end]
            if dat_urls:
                urls = dat_urls + res_urls
                return (data_path, urls)
            else:
                print("There is no turnstile data in the directory.")
        else:
            print("Could not find directory {}".format(data_path))

        txt = input("Download text files first? "
                    "If not will not keep a copy of text files. (y/n)").lower()
        if txt == 'y':
            data_path = super().download_to_txt(path=data_path, keep_urls=True)
            urls = os.listdir(data_path)
            return (data_path, urls)
        elif txt == 'n':
            print("Download from urls, might take some time...")
            self.local = False
            data_path = ''
            urls = super().get_urls()
            return (data_path, urls)
        else:
            raise ValueError("Not valid input.")

    def download_to_db(self, data_path='data', update=False):
        """
        Download mta turnstile data and write to db
        """
        data_path, urls = self.init_data(data_path, update)
        self.init_db()
        self.init_tables()
        con = self.conn_db()
        c = con.cursor()
        c.execute('SELECT file FROM file_names;')
        get_efiles = c.fetchall()
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
                    c = writeDB(dbtype=self.dbtype, filename=fname,
                                data=data, c=c)
                    iq_fn = dbInsert('file_names', ['file'], '%s',
                                     dbtype=self.dbtype)
                    if self.dbtype == 'sqlite':
                        fname = tuple([None] + [fname])
                    else:
                        fname = tuple([fname])
                    c.execute(iq_fn, fname)
                    con.commit()
                    i += 1
                except Exception as e:
                    if con:
                        con.close()
                    print(e)
                    raise

            if i > 0 and i % 5 == 0:
                print("Added {} files to the database...".format(i))
        con.commit()
        con.close()
        print("Finish writing {} data files to the database.".format(i))
        return

    def init_namekeys(self, data_path='data', update=False):
        """
        Download mta turnstile name keys and write to database
        """
        data_path, urls = self.init_data(data_path, update)
        self.init_db()
        self.init_tables()
        con = self.conn_db()

        url = [u for u in urls if u.endswith('xls')][0]
        if self.local:
            url = os.path.join(data_path, url)
        try:
            res_data = pd.read_excel(url, header=0)
            res_data.columns = ['remote', 'booth', 'station',
                                'line', 'division']
            res_data.reset_index(inplace=True)
            res_data.columns.values[0] = 'id'
            if self.dbtype == 'sqlite':
                res_data.to_sql('name_keys', con=con,
                                if_exists="replace", index=False)
            elif self.dbtype == 'postgres':
                eng = ("postgresql+psycopg2://{}:{}@{}:{}/{}"
                       .format(self.dbparms['user'],
                               self.dbparms['password'],
                               self.dbparms['host'],
                               self.dbparms['port'],
                               self.dbparms['dbname']))
                engine = create_engine(eng)
                res_data.to_sql('name_keys', con=engine,
                                if_exists="replace", index=False)
        except Exception as e:
            if con:
                con.close()
            print(e)
            raise

        con.commit()
        con.close()
        print("Added name_keys file to the database, reset if exists.")
        return
