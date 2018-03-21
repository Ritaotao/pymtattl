from pymtattl.download import (BaseDownloader, SqliteDownloader,
                               PostgresDownloader)


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
    sqlite_downloader = SqliteDownloader(start=141011, end=141025,
                                         dbparms=pm)
    sqlite_downloader.init_namekeys(path='test', update=False)
    sqlite_downloader.download_to_db(path='test', update=False)
    return
sqlite_example()


def postgres_example():
    pm = {'dbname': '',
          'user': 'ritaotao',
          'password': '1936',
          'host': 'localhost',
          'port': '5432'}
    postgres_downloader = PostgresDownloader(start=None, end=None,
                                             dbparms=pm)

    postgres_downloader.init_namekeys(path='C:\\Users\\Owner\\Desktop\\'
                                           'mta\\data',
                                      update=True)

    postgres_downloader.download_to_db(path='C:\\Users\\Owner\\Desktop\\'
                                            'mta\\data',
                                       update=True)
    return
# postgres_example()
