from pymtattl.download import MTADownloader

mta_downloader = MTADownloader(work_dir="C:\\Users\\Ruitao.Cheng\\Desktop\\mta",
                               start=140830, end=141025)
urls = mta_downloader._get_urls(keep_urls=True)

dat_dir = mta_downloader.download_to_txt(data_folder='testdata')

db_path = mta_downloader.download_to_db()
