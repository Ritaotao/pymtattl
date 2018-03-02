# pymtattl

## Introduction

Download and store MTA Turnstile Data in text files or a SQLite database

Automate downloading of turnstile entry/exit data from MTA. Allow to save data within requested time frame into a SQlite database and reshape data prior to 10/18/2014 into a more "relational" format. Considering to support more database types.

MTA Turnstile Data: http://web.mta.info/developers/turnstile.html


## Table of Contents

* [Installation](#installation)

* [Download](#download)

  * [Urls](#urls)

  * [Text Files](#text-files)

  * [SQlite Database](#sqlite-database)

## Installation

    pip install pymtattl

## Requirements

* Written for Python 3! Feel free to test and contribute using Python 2!
* Requires bs4, Pandas

## Download

### Initate MTADownloader

    from pymtattl.download import MTADownloader
    mta_downlowder = MTADownloader(work_dir='Current', start=141018, end=None)

Parameters

* `work_dir`

  - Type: String
  - specify full folder directory to store downloaded data
  - `'Current'`: default, uses current working directory `os.getcwd()`
  - Or a specific valid directory

* `start/end`

  - Type: Integer or None
  - define the date range to pull data files *(recommend testing with small date ranges, as downloading all files might be slow)*
  - Example (yymmdd) for 2014-10-18: `141018`

### Urls

Get **urls** for data within date range and resource files (description, name key)

    urls = mta_downloader._get_urls(keep_urls=True)

* Set `keep_url = True` to save the returned urls in a text file *data_urls.txt*.

* Returns list of url strings

### Text Files

Download requested data as separate **text files**

    dat_dir = mta_downloader.download_to_txt()

* Default create and store in a new folder **data** under working directory

* `data_folder` *(optional)*: provide a custom folder name

* Returns data folder directory

### SQlite Database

Download, reformat, and store requested data in a **SQLite database**

    db_path = mta_downloader.download_to_db()

* Create a SQlite database **data.db** under working directory and 3 tables

  - **turnstile**: holds turnstile data
  - **name_keys**: a matching table to lookup station name given remote and booth
  - **file_names**: names of data files that are already in **turnstile** table

* `data_path`:

  - `''` | `None`: must run `download_to_txt()` first, then the function uses text files within data folder directory (instance attribute)
  - Otherwise, specify a full data folder directory to search for existing data text files

* If can not find local text files, choose to download text files first or directly store in the database (use with caution, could be very slow!)

* Returns database directory

More to come...
