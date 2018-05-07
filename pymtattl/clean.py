import os
from datetime import datetime
from urls import get_data_urls


class Cleaner:
    """
    download, process, and save mta turnstile data page
    """
    def __init__(self):
        pass

    def getUrls(self, start, end):
        start = int(datetime.strptime(start,'%Y-%m-%d').strftime('%y%m%d'))
        end = int(datetime.strptime(end,'%Y-%m-%d').strftime('%y%m%d'))
        urls = get_data_urls(start, end)
        return urls


if __name__ == "__main__":
    cleaner = Cleaner()
    urls = cleaner.getUrls(start='2014-05-13',
                           end='2015-05-13')
    print("Data url example: ", urls[0])
