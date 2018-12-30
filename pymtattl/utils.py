import os
import sys
import pandas as pd
from .sqlalchemy_declarative import Station, data_frame
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import INTEGER
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
    df_station = data_frame(session.query(Station), ['id', 'ca', 'unit'])
    print(df_station.head())
    session.close()

    # join station df with map df, left_on: ca, unit, right_on: Booth, Remote
    df_join = df_station.merge(df_map, how='left', left_on=['ca', 'unit'], right_on=['Booth', 'Remote'])
    df_join.drop(['Booth', 'Remote'], axis=1, inplace=True)
    df_join.rename(
        index=str,
        columns={
            'Station': 'name',
            'Line Name': 'line',
            'Division': 'division',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
        },
        inplace=True
    )
    df_join = df_join[['id','ca','unit','name','line','division','latitude','longitude']]
    print(df_join)

    # can't directly replace Station table since Device (foreign key) depens on Station
    # https://stackoverflow.com/questions/31988322/pandas-update-sql
    df_join.to_sql('temp_table', con=engine, index=False, if_exists='replace')
    
    sql = """
        UPDATE station AS s
        SET name = t.name,
            line = t.line,
            division = t.division,
            latitude = t.latitude,
            longitude = t.longitude
        FROM temp_table AS t
        WHERE s.id = t.id;
    """

    with engine.begin() as conn:
        conn.execute(sql)
        conn.execute('DROP TABLE temp_table;')

    print("Geomapped station table udpated.")