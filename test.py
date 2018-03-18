import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from pymtattl.download import BaseDownloader, DBDownloader


class BaseDownloaderTest(TestCase):

    def setUp(self):
        self.dn = BaseDownloader(start=141011, end=141025)
        self.path = 'test'
        self.url_dir = os.path.join(os.getcwd(), self.path)
        self.url_path = os.path.join(self.url_dir, 'data_urls.txt')

    def test_get_not_keep_urls(self):
        urls = self.dn.get_urls(path='', keep_urls=False)
        self.assertEqual(len(urls), 6)

    def test_get_and_keep_urls(self):
        with patch('builtins.input', return_value='y'):
            self.dn.get_urls(path=self.path, keep_urls=True)
            self.assertTrue(os.path.exists(self.url_path))
            with open(self.url_path, 'r') as f:
                for i, l in enumerate(f):
                    pass
            self.assertEqual(i+1, 6)
        shutil.rmtree(self.url_dir, ignore_errors=False, onerror=None)

    def test_download_to_txt(self):
        with patch('builtins.input', return_value='y'):
            dat_dir = self.dn.download_to_txt(path=self.path, keep_urls=False)
            self.assertTrue(os.path.exists(dat_dir))
            self.assertEqual(len(os.listdir(dat_dir)), 6)
        shutil.rmtree(dat_dir, ignore_errors=False, onerror=None)


class DBDownloaderSqliteTest(TestCase):

    def setUp(self):
        self.pm = {'path': 'test1'}
        self.dn = DBDownloader(start=141011, end=141025,
                               dbtype='sqlite',
                               dbparms=self.pm)

    def test_auth_db(self):
        se = ['y', 'test', 'y']
        with patch('builtins.input', side_effect=se):
            dbparms = self.dn.auth_db(self.pm)
            self.assertTrue(self.dn.new)
            self.assertIn('dbname', dbparms.keys())
            self.assertTrue(os.path.isdir(dbparms['path']))
            os.rmdir(dbparms['path'])

    def test_build_con(self):
        se = ['y', 'test', 'y']
        with patch('builtins.input', side_effect=se):
            dbparms = self.dn.auth_db(self.pm)
            conn = os.path.join(dbparms['path'], 'test.db')
            self.dn.build_conn(dbparms)
            self.assertEqual(conn, self.dn.conn_string)
            os.rmdir(dbparms['path'])
