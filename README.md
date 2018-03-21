# pymtattl

## Introduction

Download and store MTA Turnstile Data

Automate downloading turnstile entry/exit data from MTA website. Save as text files, or directly write to a SQLite/Postgres Database. Can also specify a requested time frame as the earliest files went back to 2010.

MTA Turnstile Data: http://web.mta.info/developers/turnstile.html


## Table of Contents

* [Installation](#installation)

* [Download](#download)

  * [Text Files](#text-files)

  * [SQLite Database](#sqlite-database)

  * [Postgres Database](#postgres-database)

* [Caveats](#caveats)

* [To-Do](#to-do)

## Installation

    pip install pymtattl

## Requirements

* Written for Python 3! Feel free to test and contribute using Python 2!
* Requires bs4, pandas, psycopg2

## Download Methods

### Text Files

BaseDownloader: Download requested data as separate **text files**

    from pymtattl.download import BaseDownloader
    base_downlowder = BaseDownloader(start=141018, end=None)    
    dat_dir = base_downloader.download_to_txt(path='data', keep_urls=False)  

* `start/end`: *integer or None*
  - Define the date range to pull data files *(recommend testing with small date ranges, as downloading all files might be slow)*
  - Example (yymmdd) for 2014-10-18: `141018`

* `path`: *string*
  - An existing directory to save downloaded data files
  - Can also put an empty string (to save under current working directory) or a new folder name (ie. 'data')

* `keep_urls`: *boolean*
  - If true will include retrieved urls in **data_urls.txt** under provided directory

* Returns data folder directory

### SQLite Database

SqliteDownloader: Reformat data either from **local path** or directly downloaded from MTA website and save in a SQLite database

    from pymtattl.download import SqliteDownloader
    # provide database parameters
    pm = {'path': 'test',
          'dbname': 'testdb'}
    sqlite_downloader = SqliteDownloader(start=141018, end=None, dbparms=pm)
    # download data files and save to sqlite db
    sqlite_downloader.download_to_db(path='data', update=False)
    # write name_keys file to db
    sqlite_downloader.init_namekeys(path='data', update=False)

* Create (if not exists) a SQLite database **testdb.db** under **~/test/** and 3 tables

  - **turnstile**: holds turnstile data
  - **name_keys**: a matching table to lookup station name given remote and booth
  - **file_names**: names of data files that are already in **turnstile** table

* `start/end`: *integer or None*
  - Define the date range to pull data files *(recommend testing with small date ranges, as downloading all files might be slow)*
  - Example (yymmdd) for 2014-10-18: `141018`

* `dbparm`: *dict*
  - `path`: path to create or find an existing sqlite database file
  - `dbname`: database file name to create or save to if exists

* `path`: *string*
  - Local data folder path if data already downloaded
  - Specify an existing directory or a new folder name to store downloaded text files
  - Can also choose to directly read from MTA website and write to db, as if there is no local data files

* Returns data folder directory

### Postgres Database

PostgresDownloader: Reformat data either from **local path** or directly downloaded from MTA website and save in a Postgres database

    from pymtattl.download import PostgresDownloader
    # provide database parameters
    pm = {'dbname': '',
          'user': 'a',
          'password': 'b',
          'host': 'localhost',
          'port': '5432'}
    postgres_downloader = PostgresDownloader(start=141018, end=None, dbparms=pm)
    # download data files and save to postgres db
    postgres_downloader.download_to_db(path='data', update=False)
    # write name_keys file to db
    postgres_downloader.init_namekeys(path='data', update=False)

* Create (if not exists) a Postgres database and 3 tables

  - **turnstile**: holds turnstile data
  - **name_keys**: a matching table to lookup station name given remote and booth
  - **file_names**: names of data files that are already in **turnstile** table

* `start/end`: *integer or None*
  - Define the date range to pull data files *(recommend testing with small date ranges, as downloading all files might be slow)*
  - Example (yymmdd) for 2014-10-18: `141018`

* `dbparm`: *dict*
  - `dbname`: database name to connect, if empty string or remove from the dict, will prompt to ask for new database name to **create**
  - `user`|`password`|`host`|`port`: parameters to connect to Postgres instance

* `path`: *string*
  - Local data folder path if data already downloaded
  - Specify an existing directory or a new folder name to store downloaded text files
  - Can also choose to directly read from MTA website and write to db, as if there is no local data files

## Caveats

* Some know data issues and these rows will be skipped while building the database

  - In Turnstile_120428.txt, one line with empty ('') exit number
  - In Turnstile_120714.txt, first few lines could not be parsed
  - It seems recently date strings were reformatted to `mm/dd/yyyy` (03/20/2018)

## To-Do

* De-cumulate entry and exit numbers, and store data within selected date range into a new table

* A Summary table (ie. number of booth per station, average daily station entries/exits, ...) for "cleaned" data table above

* More to come...
