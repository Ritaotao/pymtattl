import os
import sys
import pandas as pd
from .sqlalchemy_declarative import Station, data_frame
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import INTEGER
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import create_engine


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