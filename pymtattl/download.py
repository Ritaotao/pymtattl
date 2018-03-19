from __future__ import absolute_import, print_function, unicode_literals

import os
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from .utils import (createFolder, parseRows, getPubDate, filterUrls,
                    notEmptyStr, strParms)
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
                    for u in urls if u.get('href').startswith('data')]
        dat_urls = filterUrls(dat_urls, self.start, self.end)

        # resource file urls (headers, lookup tables for station names)
        res_urls = [self.__class__.MAIN_URL + u.get('href')
                    for u in urls
                    if u.get('href').startswith('resources')]

        print("{} data files and {} resource files available for download."
              .format(len(dat_urls), len(res_urls)))
        urls = dat_urls + res_urls
        if keep_urls:
            path = createFolder(path)
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
        dat_dir = createFolder(path)
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


class SqliteDownloader(BaseDownloader):
    """ Downlaod mta turnstile data and save to database """

    def __init__(self, start=None, end=None, dbparms={}):
        super().__init__(start, end)
        self.dbparms = dbparms
        self.conn_string = ''
        self.local = True

    def auth_db(self, dbparms):
        """ Authenticate db arguments """
        if not('dbname' in dbparms.keys() and notEmptyStr(dbparms['dbname'])):
            create = input("No dbname provided. "
                           "Do you want to create a new db? (y/n)").lower()
            if create == 'y':
                dbparms['dbname'] = input("Please enter a valid dbname:")
            else:
                raise ValueError("<dbname> required.")

        assert 'path' in dbparms.keys(), ("<path> required "
                                          "in sqlite parameters.")
        dbparms['path'] = createFolder(dbparms['path'])
        return dbparms

    def build_conn(self, dbparms):
        """ Form connection argument for db """
        dbname = dbparms['dbname']
        if dbname.split('.')[-1] != 'db':
            dbname += '.db'
        self.conn_string = os.path.join(dbparms['path'], dbname)
        return dbname

    def conn_db(self):
        """ Connect to db """
        con = sqlite3.connect(self.conn_string)
        return con

    def init_db(self):
        """ Init database and tables """
        dbparms = self.auth_db(self.dbparms)
        dbname = self.build_conn(dbparms)

        if os.path.exists(self.conn_string):
            print('Database <{}> exists.'.format(dbname))
        else:
            print('Creating new database <{}>'.format(dbname))
        try:
            con = self.conn_db()
        except Exception as e:
            print(e)
            raise

        c = con.cursor()
        pk = 'id INTEGER PRIMARY KEY'
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
        print('Created tables if not exist: '
              '<turnstiles>, <name_keys>, <file_names>')
        return

    def init_data(self, path='data', update=False, filetype='data'):
        """ Moniter and acquire online or local files """
        if update:
            path = super().download_to_txt(path=path, keep_urls=True)
        if os.path.isdir(path):
            urls = os.listdir(path)
            if filetype == 'data':
                urls = [u for u in urls if u.startswith('turnstile')]
                urls = filterUrls(urls, self.start, self.end)
            elif filetype == 'resource':
                urls = [u for u in urls if u.endswith('xls')][0]
            else:
                raise ValueError("Not valid filetype.")
            if urls:
                return (path, urls)
            else:
                print("There is no requested file in the directory.")

        txt = input("Download text files? "
                    "If not will not keep downloaded files. (y/n)").lower()
        if txt == 'y':
            return self.init_data(path=path, update=True, filetype=filetype)
        elif txt == 'n':
            print("Download from urls, might take some time...")
            self.local = False
            path = ''
            urls = super().get_urls()
            return (path, urls)
        else:
            raise ValueError("Not valid input.")

    def download_to_db(self, path='data', update=False):
        """ Download mta turnstile data and write to db """
        path, urls = self.init_data(path=path, update=update, filetype='data')
        self.init_db()
        con = self.conn_db()
        c = con.cursor()
        c.execute('SELECT file FROM file_names;')
        get_efiles = c.fetchall()
        exist_files = [i[0] for i in get_efiles]
        iq1 = """INSERT INTO turnstiles (booth, remote, scp, date, time,
                 description, entries, exits) VALUES (?,?,?,?,?,?,?,?);"""
        iq2 = """INSERT INTO file_names (file) VALUES (?);"""
        i = 0
        for url in urls:
            fname = url.split('/')[-1]
            if fname not in exist_files and fname.startswith('turnstile'):
                if self.local:
                    url = os.path.join(path, url)
                    with open(url, 'r') as f:
                        data = f.read().split('\n')
                else:
                    data = urlopen(url).read().decode('utf-8').split('\n')
                try:
                    rows = parseRows(fname, data)
                    if rows:
                        c.executemany(iq1, rows)
                        c.execute(iq2, (fname,))
                        con.commit()
                        i += 1
                    else:
                        print("No data parsed from {}".format(fname))
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

    def init_namekeys(self, path='data', update=False):
        """
        Download mta turnstile name keys and write to database
        """
        path, urls = self.init_data(path=path, update=update,
                                    filetype='resource')
        self.init_db()
        con = self.conn_db()
        if self.local:
            urls = os.path.join(path, urls)
        try:
            res_data = pd.read_excel(urls, header=0)
            res_data.columns = ['remote', 'booth', 'station',
                                'line', 'division']
            res_data.reset_index(inplace=True)
            res_data.columns.values[0] = 'id'
            res_data.to_sql('name_keys', con=con,
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


class PostgresDownloader(BaseDownloader):
    """
    Downlaod mta turnstile data and save to database
    """

    def __init__(self, start=None, end=None, dbparms={}):
        super().__init__(start, end)
        self.dbparms = dbparms
        self.new = False
        self.conn_string = ''
        self.local = True

    def auth_db(self, dbparms):
        """ Authenticate db arguments """
        if not('dbname' in dbparms.keys() and notEmptyStr(dbparms['dbname'])):
            create = input("No dbname provided. "
                           "Do you want to create a new db? (y/n)").lower()
            if create == 'y':
                dbparms['dbname'] = input("Please enter a valid dbname:")
                self.new = True
            else:
                raise ValueError("<dbname> required.")
        assert 'user' in dbparms.keys(), ("<user> required "
                                          "in postgres parameters.")
        assert 'password' in dbparms.keys(), ("<password> required "
                                              "in postgres parameters.")
        assert 'host' in dbparms.keys(), ("<host> required "
                                          "in postgres parameters.")
        assert 'port' in dbparms.keys(), ("<port> required "
                                          "in postgres parameters.")
        return dbparms

    def build_conn(self, dbparms):
        """ Form connection argument for db """
        dbname = dbparms['dbname']
        if self.new:
            pre_Parms = {k: ('postgres' if k == 'dbname' else dbparms[k]) for k, v in dbparms.items()}
            self.conn_string = strParms(pre_Parms)
        else:
            self.conn_string = strParms(dbparms)
        return dbname

    def conn_db(self):
        con = psycopg2.connect(dsn=self.conn_string)
        return con

    def init_db(self):
        """ Init database and tables """
        dbparms = self.auth_db(self.dbparms)
        dbname = self.build_conn(dbparms)

        try:
            con = self.conn_db()
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
        except Exception as e:
            print(e)
            raise

        c = con.cursor()
        pk = 'id SERIAL PRIMARY KEY'
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
        print('Created tables if not exist: '
              '<turnstiles>, <name_keys>, <file_names>')
        return

    def init_data(self, path='data', update=False, filetype='data'):
        """ Moniter and acquire online or local files """
        if update:
            path = super().download_to_txt(path=path, keep_urls=True)
        if os.path.isdir(path):
            urls = os.listdir(path)
            if filetype == 'data':
                urls = [u for u in urls if u.startswith('turnstile')]
                urls = filterUrls(urls, self.start, self.end)
            elif filetype == 'resource':
                urls = [u for u in urls if u.endswith('xls')][0]
            else:
                raise ValueError("Not valid filetype.")
            if urls:
                return (path, urls)
            else:
                print("There is no requested file in the directory.")

        txt = input("Download text files? "
                    "If not will not keep downloaded files. (y/n)").lower()
        if txt == 'y':
            return self.init_data(path=path, update=True, filetype=filetype)
        elif txt == 'n':
            print("Download from urls, might take some time...")
            self.local = False
            path = ''
            urls = super().get_urls()
            return (path, urls)
        else:
            raise ValueError("Not valid input.")

    def download_to_db(self, path='data', update=False):
        """ Download mta turnstile data and write to db """
        path, urls = self.init_data(path=path, update=update, filetype='data')
        self.init_db()
        con = self.conn_db()
        c = con.cursor()
        c.execute('SELECT file FROM file_names;')
        get_efiles = c.fetchall()
        exist_files = [i[0] for i in get_efiles]
        iq1 = """INSERT INTO turnstiles (booth, remote, scp, date, time,
                 description, entries, exits) VALUES (?,?,?,?,?,?,?,?);"""
        iq2 = """INSERT INTO file_names (file) VALUES (?);"""
        i = 0
        for url in urls:
            fname = url.split('/')[-1]
            if fname not in exist_files and fname.startswith('turnstile'):
                if self.local:
                    url = os.path.join(path, url)
                    with open(url, 'r') as f:
                        data = f.read().split('\n')
                else:
                    data = urlopen(url).read().decode('utf-8').split('\n')
                try:
                    rows = parseRows(fname, data)
                    if rows:
                        c.executemany(iq1, rows)
                        c.execute(iq2, (fname,))
                        con.commit()
                        i += 1
                    else:
                        print("No data parsed from {}".format(fname))
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

    def init_namekeys(self, path='data', update=False):
        """
        Download mta turnstile name keys and write to database
        """
        path, urls = self.init_data(path=path, update=update,
                                    filetype='resource')
        self.init_db()
        con = self.conn_db()
        if self.local:
            urls = os.path.join(path, urls)
        try:
            res_data = pd.read_excel(urls, header=0)
            res_data.columns = ['remote', 'booth', 'station',
                                'line', 'division']
            res_data.reset_index(inplace=True)
            res_data.columns.values[0] = 'id'
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
