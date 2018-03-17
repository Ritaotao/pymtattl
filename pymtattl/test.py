import os
import shutil
from unittest import TestCase
from unittest.mock import patch
from download import BaseDownloader, DBDownloader
"""
base_downloader = BaseDownloader(work_dir='test',
                                 start=141011, end=141025)
print("Running get url test...")
print("///")
urls = base_downloader.get_urls(keep_urls=True)
assert len(urls) == 6, "Number of urls is not expected."
print("///")

print("Running download txt files test...")
print("///")
dat_dir = base_downloader.download_to_txt(data_folder='test')
assert len(os.listdir(dat_dir)) == 6,\
       "Number of downloaded text files is not correct"
print("///")
print("Test is done. Cleaning up...")

shutil.rmtree(base_downloader.work_dir, ignore_errors=False, onerror=None)
"""


class DBDownloaderSqliteTest(TestCase):

    def setUp(self):
        self.pm = {'dbname': 'test'}
        self.dn = DBDownloader(work_dir='test',
                               start=141011, end=141025,
                               dbtype='sqlite',
                               dbparms=self.pm)

    def test_createDir(self):
        self.assertTrue(os.path.isdir(self.dn.work_dir))

    def test_authdb(self):
        self.assertIn('dbname', self.dn.dbparms.keys())
        val = self.dn.dbparms['dbname']
        self.assertIsInstance(val, str)
        self.assertNotEqual(val, '')
