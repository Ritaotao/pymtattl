import os
import shutil
from download import DBDownloader
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
db_parms = {'dbname':'test'}
sqlite_downloader = DBDownloader(work_dir='test',
                                 start=141011, end=141025, dbtype='sqlite', dbparms=db_parms)
print(sqlite_downloader.work_dir)
sqlite_downloader.download_to_db('test')
