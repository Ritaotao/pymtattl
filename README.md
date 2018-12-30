# pymtattl

## Introduction

MTA Turnstile Data: http://web.mta.info/developers/turnstile.html

Download, process, and store MTA Turnstile Data in database

* `Downloader` class: automate downloading turnstile raw entry/exit data from MTA website into txt files (weekly, cumulated)
* `Cleaner` class: convert downloaded text files and write decumulated records to database.

Note 1: trying to be database agnostic, used sqlalchemy and tested with sqlite and postgres 10.

Note 2: be cautious about date range of files need to be appended to the database tables, avoid duplication or adding data of weeks prior to the ones in the tables.

## Table of Contents

* [Installation](#installation)

* [Download](#download)

* [Clean](#clean)

* [Caveats](#caveats)

* [To-Do](#to-do)

## Installation

    pip install pymtattl

## Requirements

* Written for Python 3! Feel free to test and contribute using Python 2!
* Requires bs4, pandas, sqlalchemy

## Download

`Downloader`: download data within date range as weekly text files.

    from pymtattl import Downloader

    download = Downloader(date_range=("2018-01-01", "2018-02-01"),
                          main_path='./data/',
                          verbose=10)
    data_path = download.run()

* `date_range`: *tuple*
  - Define the start and end dates *(recommend testing with small date ranges, as downloading all files might be slow)*
  - Example (yyyy-mm-dd): `("2018-01-01", "2018-02-01")`

* `main_path`: *string*, default './data/'
  - A directory to store downloaded data files (will be created if dir not exists)
  - Every run creates a new dir `download-yyyymmddhhmmss`, where all data files and log files are nested under

* `verbose`: *int*, default 10
  - Log and print out when every n files are downloaded

* Returns full directory of parent folder `download-yyyymmddhhmmss`

## Clean

`Cleaner`: decumulate and store downloaded data files in database. Please make sure database already exists if not using sqlite.

    from pymtattl import Cleaner
    
    clean = Cleaner(date_range=None,
                    input_path='./data/download-20181227160016',
                    dbstring='postgresql://user:p@ssword@localhost:5432/mta_sample')
    clean.run()

* Create 4 tables to save disk space and use end of last week numbers to be used as baseline for current week
  - `turnstile`: decumulated entry/exit 
    - columns: *id, device_id, timestamp, description, entry, exit*
  - `station`: mta staion defined by ca, unit pairs
      - columns: *id, ca, unit*
  - `device`: device location in each station
      - columns: *id, station_id, scp*
  - `previous`: memorize ending data from previous week, support decumulate accross weekly files
      - columns: *id, device_id, timestamp, description, entry, exit, file_date*

* `date_range`: *tuple*, default None
  - Define the start and end dates of the files to be added to database
  - Example (yyyy-mm-dd): `("2018-01-01", "2018-02-01")`
  - If None (default), will add all data files in folder

* `input_path`: *string*
  - Directory of the downloaded text files to be added to database

* `dbstring`: *string*
  - Database urls used by sqlalchemy
  - dialect+driver://username:password@host:port/database
  - postgres: 'postgresql://scott:tiger@localhost/mydatabase'
  - mysql: 'mysql://scott:tiger@localhost/foo'
  - sqlite: 'sqlite:///foo.db'
  - more info: https://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql

## Data Issues

* Some known data issues, might happen in multiple files and quite manual to detect and remove

  - In turnstile_120428.txt, one line with empty ('') exit number
  - In turnstile_120714.txt, first few lines could not be parsed
  - Date strings were reformatted to `mm/dd/yyyy` (03/20/2018)
  - In turnstile_170204.txt, A025, R023, 01-03-01, 02/01/2017, entry numbers start counting backwards
  - In turnstile_170909.txt, C020, R233, 00-00-00, 09/02/2017, status switch between REGULAR and RECOVER AUD
  - In turnstile_170318.txt, PTH03, R522, 00-00-09, 03/11/2017, every second record seems to be correct but every next one could increase by 80K. This seem to happen with smaller numbers as well. In turnstile_171028.txt, PTH07, R550, 00-01-06, 10/21/2017, entries increase and decrease by 8K.

* Incompatible data types and formats were detected, logged, and ignored during Downloading process. The package provides a workaround with other issues related to values:

  - Count backwards: use absolute values after diff method is called
  - Adjacent values inconsistent, but every second record correct: a second diff is called on values greater than certain threshold (Entry > 7000, Exit > 6000)
  - Huge values: values still above threshold are dropped

## To-Do

* Batch processing of multiple data files together before decumulate step.

* Append station name to station table. (in pymtattl/utils.py)

* More to come...
