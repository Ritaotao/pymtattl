from pymtattl.download import BaseDownloader, DBDownloader


def base_example():
    """Run base example"""
    base_downloader = BaseDownloader(start=141011, end=141025)
    urls = base_downloader.get_urls()
    print(urls[0])
    dat_dir = base_downloader.download_to_txt(path='test', keep_urls=False)
    print(dat_dir)
    return


def sqlite_example():
    """Run Sqlite example"""
    pm = {'path': 'test',
          'dbname': 'test'}
    sqlite_downloader = DBDownloader(start=141011, end=141025,
                                     dbtype='sqlite', dbparms=pm)
    sqlite_downloader.download_to_db(data_path='data', update=False)
    return
# sqlite_example()


def postgres_example():
    pm = {'dbname': 'mta1',
          'user': 'b',
          'password': 'c',
          'host': 'localhost',
          'port': '5432'}
    postgres_downloader = DBDownloader(start=141011, end=141025,
                                       dbtype='postgres', dbparms=pm)
    postgres_downloader.init_namekeys(data_path='C:\\Users\\Owner\\'
                                                'Desktop\\mta\\data',
                                      update=True)
    """
    postgres_downloader.download_to_db(data_path='C:\\Users\\Owner\\'
                                                 'Desktop\\mta\\data',
                                       update=True)
    """
    return


postgres_example()
