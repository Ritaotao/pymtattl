import os
import sys
import pandas as pd
import logging
from datetime import datetime

from .sqlalchemy_declarative import Station, data_frame
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import INTEGER
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import create_engine

def createLogger(prefix, log_path=None):
    """create logging instance"""
    LOGFORMAT = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    logging.basicConfig(level=logging.INFO, format=LOGFORMAT)
    
    appLogger = logging.getLogger(prefix)
    appLogger.addHandler(logging.NullHandler())
    # write log to file
    if log_path:
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(LOGFORMAT))
        appLogger.addHandler(fh)
    return appLogger

def str2intDate(datetext):
    """take string date format input and convert to numerics"""
    return int(datetime.strptime(datetext, '%Y-%m-%d').strftime('%y%m%d'))

def parseDate(name):
    """return date part (yymmdd) given data url or file name"""
    return int(name.split('_')[-1].split('.')[0])

def filterUrl(urls, date_range):
    return [u for u in urls if parseDate(u) >= date_range[0] and parseDate(u) <= date_range[1]]

def createPath(path):
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except Exception as e:
            raise e
    return path

def station_mapping(file_path, dbstring):
    # read geocoded Remote-Booth-Station.xlsx file
    # Thanks to Chris Whong and Mala Hertz for mapping Remote Unit with the Latitude/Longitude
    # repo here: https://github.com/chriswhong/nycturnstiles
    if not os.path.isfile(file_path):
        print("Mapping file not found.")
        sys.exit(1)
    df_map = pd.read_excel(file_path)
    print(df_map.head())

    # connect to db and read in Station table
    engine = create_engine(dbstring)
    Session = sessionmaker(bind=engine)
    session = Session()
    #df_station = data_frame(session.query(Station), ['id', 'ca', 'unit'])
    #print(df_station.head())

    for _, row in df_map.iterrows():
        try:
            obj = session.query(Station).filter(Station.ca==row['Booth']).filter(Station.unit==row['Remote']).one()
            obj.name = row['Station']
            obj.line = row['Line Name']
            obj.division = row['Division']
            obj.latitude = row['Latitude']
            obj.longitude = row['Longitude']
            session.commit()
        except NoResultFound:
            pass

    session.close()

    print("Geomapped station table udpated.")