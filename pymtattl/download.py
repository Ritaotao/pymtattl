from __future__ import absolute_import, print_function, unicode_literals

import os
from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from pymtattl.utils import createFolder, writeDB, getPubDate
import sqlite3
import pandas as pd


class MTADownloader:
    """
    Download mta turnstile data page
    """
    MAIN_URL = 'http://web.mta.info/developers/'
    TURNSTILE_URL = 'http://web.mta.info/developers/turnstile.html'

    def __init__(self, work_dir='Current',
                 start=141018, end=None):
        # define a valid folder for download data
        self.work_dir = createFolder(work_dir, branch=None)
        assert (start is None or type(start) == int), \
            'Start should be None or 6-digit integer (yymmdd)'
        assert (end is None or type(end) == int), \
            'End should be None or 6-digit integer (yymmdd)'
        self.start = start
        self.end = end
        self.dat_dir = ''

    def _get_urls(self, url=TURNSTILE_URL, keep_urls=False):
        """
        Get and save data urls
        """
        soup = BeautifulSoup(urlopen(url), 'lxml')
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

        if keep_urls:
            file_path = os.path.join(self.work_dir, 'data_urls.txt')
            with open(file_path, 'w+') as f:
                f.write('\n'.join(dat_urls))
                f.write('\n'.join(res_urls))
        return dat_urls, res_urls

    def download_to_txt(self, data_folder='data'):
        """
        Download text files
        """
        dat_dir = createFolder(self.work_dir, data_folder)
        dat_urls, res_urls = self._get_urls()
        # url=self.__class__.TURNSTILE_URL
        urls = dat_urls + res_urls
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
        print("Added {} text files to {}. (Current # of Files in Dir: {})"
              .format(i, dat_dir, len(os.listdir(dat_dir))))
        self.dat_dir = dat_dir
        return dat_dir

    def init_db(self):
        """
        Create database or update missing tables if missing
        """
        db_path = os.path.join(self.work_dir, 'data.db')
        new = False
        if not os.path.exists(db_path):
            new = True
        try:
            conn = sqlite3.connect(db_path)
        except Exception as e:
            print(e)
            raise
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
        if new:
            print('Created: {}.'.format(db_path))
        return db_path

    def download_to_db(self, data_path=''):
        """
        Download data and store in a sqlite database
        """
        local = True
        if data_path != '':
            self.dat_dir = data_path
        try:
            data_urls = os.listdir(self.dat_dir)
        except Exception as e:
            print("Couldn't find local data folder.")
            txt = input("Download text files first? (Y/N)").lower()
            if txt == 'y':
                self.download_to_txt()
                data_urls = os.listdir(self.dat_dir)
            elif txt == 'n':
                print("Download from urls, might take some time...")
                dat_urls, res_urls = self._get_urls()
                # url=self.__class__.TURNSTILE_URL
                data_urls = dat_urls + res_urls
                local = False
            else:
                print("Not valid input.")
                return

        db_path = self.init_db()
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        get_efiles = c.execute('SELECT file FROM file_names').fetchall()
        exist_files = [i[0] for i in get_efiles]

        i = 0
        for url in data_urls:
            fname = url.split('/')[-1]
            if fname not in exist_files and fname.startswith('turnstile'):
                if local:
                    with open(os.path.join(self.dat_dir, url), 'r') as f:
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
                    url = os.path.join(self.dat_dir, url)
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
                print("Added {} files to the database...".format(i))
        conn.commit()
        conn.close()
        print("Wrote {} files to the database.".format(i))
        return
