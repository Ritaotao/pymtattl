from __future__ import absolute_import, print_function, unicode_literals

from .utils import search_geocoding, strParms, notEmptyStr
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class PostgresHelper:
    """
    Extra steps to play with mta turnstile data live in a Postgres database
    """

    def __init__(self, dbparms={}):
        self.dbparms = dbparms
        self.conn_string = ''
        self.engine = None

    def auth_db(self, dbparms):
        """ Authenticate db arguments """
        assert 'dbname' in dbparms.keys(), ("<dbname> required "
                                            "in postgres parameters")
        assert 'user' in dbparms.keys(), ("<user> required "
                                          "in postgres parameters.")
        assert 'password' in dbparms.keys(), ("<password> required "
                                              "in postgres parameters.")
        assert 'host' in dbparms.keys(), ("<host> required "
                                          "in postgres parameters.")
        assert 'port' in dbparms.keys(), ("<port> required "
                                          "in postgres parameters.")
        return

    def build_conn(self, dbparms):
        """ Form connection argument for db """
        self.conn_string = strParms(dbparms)
        return

    def conn_db(self):
        con = psycopg2.connect(dsn=self.conn_string)
        return con

    def pd_create_engine(self):
        self.auth_db(self.dbparms)
        eng = ("postgresql+psycopg2://{}:{}@{}:{}/{}"
               .format(self.dbparms['user'],
                       self.dbparms['password'],
                       self.dbparms['host'],
                       self.dbparms['port'],
                       self.dbparms['dbname']))
        self.engine = create_engine(eng)
        return

    def auth_table(self):
        self.auth_db(self.dbparms)
        self.build_conn(self.dbparms)
        con = None
        try:
            con = self.conn_db()
            c = con.cursor()
            for t in ['turnstiles', 'name_keys', 'file_names']:
                c.execute("""select exists(select * from information_schema.tables
                             where table_name=%s)""", (t,))
                if not c.fetchone()[0]:
                    con.close()
                    raise ValueError("{} not found. Create it first "
                                     "using download module.".format(t))
            print("Tables check passed.")
        except Exception as e:
            print(e)
            raise
        finally:
            if con is not None:
                con.close()
        return

    def create_table(self, table='', cols='', idpk=True):
        self.auth_db(self.dbparms)
        self.build_conn(self.dbparms)
        con = None
        try:
            con = self.conn_db()
            c = con.cursor()
            if idpk:
                pk = 'id SERIAL PRIMARY KEY, '
            else:
                pk = ''
            gs = ("CREATE TABLE IF NOT EXISTS " + table + " (" + pk
                  + cols + ");")
            c.execute(gs)
            con.commit()
            print("Table <{}> created.".format(table))
        except Exception as e:
            print(e)
            raise
        finally:
            if con is not None:
                con.close()
        return

    def append_namekeys(self, initial=True, nk_list=[]):
        if initial:
            nks = [('R194', 'R217', 'BLEECKER ST', '6DF', 'IRT'),
                   ('R001', 'R101', 'SOUTH FERRY', 'R1', 'IRT'),
                   ('R028', 'A077', 'FULTON ST', 'ACJZ2345', 'BMT'),
                   ('R028', 'A081', 'FULTON ST', 'ACJZ2345', 'BMT'),
                   ('R028', 'A082', 'FULTON ST', 'ACJZ2345', 'BMT'),
                   ('R088', 'A049', 'CORTLANDT ST', 'R', 'BMT'),
                   ('R057', 'R612', 'ATLANTIC AVE', '2345BDNQR', 'IRT'),
                   ('R028', 'N098', 'FULTON ST', '2345ACJZ', 'IRT'),
                   ('R202', 'N330', '63 DR-REGO PARK', 'MR', 'IND'),
                   ('R168', 'R169', '96 ST', '123', 'IRT'),
                   ('R014', 'N095A', 'FULTON ST', 'ACJZ2345', 'IND')]
            nk_list.extend(nks)
        else:
            assert len(nk_list) > 0, ("Please provide namekey list to append, "
                                      "if initial is set to False.")

        self.auth_table()
        self.pd_create_engine()
        max_id = pd.read_sql_query("SELECT MAX(id) FROM name_keys;",
                                   con=self.engine).iloc[0, 0] + 1
        df_nk = pd.DataFrame.from_records(data=nk_list,
                                          columns=['remote', 'booth',
                                                   'station', 'line',
                                                   'division'])
        df_nk.index = df_nk.index + max_id
        df_nk.reset_index(inplace=True)
        df_nk.columns.values[0] = 'id'
        try:
            df_nk.to_sql("name_keys", con=self.engine, if_exists='append',
                         index=False)
            print("Appended name_keys list to name_keys table.")
        except Exception as e:
            print(e)
            raise
        return

    def create_geostations(self):
        # metrocard vans could be ignored as no turnstile data
        # and doesn't sound like going to have any.
        # newark hw bmebe, newark bm bw, newark hm he no idea
        miss_station_dict = {
            "2 BDWY CUST SRV": "2 Broadway",
            "8 ST-B'WAY NYU": "East 8th Street & Broadway",
            "HOYT/SCHERMER": "Hoyt Street & Schermerhorn Street",
            "MURRAY ST-B'WAY": "Broadway/Murray St",
            "PRINCE ST-B'WAY": "Broadway/Prince St"
        }

        self.create_table(table='geo_stations',
                          cols='station text PRIMARY KEY, lat real, lng real',
                          idpk=False)
        self.auth_table()
        self.pd_create_engine()
        stations = pd.read_sql_query("SELECT DISTINCT station FROM "
                                     "name_keys;", con=self.engine)
        exist_st = pd.read_sql_query("SELECT DISTINCT station FROM "
                                     "geo_stations;", con=self.engine)
        new_set = set(stations['station']) - set(exist_st['station'])
        new_stations = {}
        miss_stations = []
        j = 0
        for s in new_set:
            try:
                new_stations[s] = search_geocoding(s)
            except IndexError as e:
                if s in miss_station_dict.keys():
                    new_stations[s] = search_geocoding(miss_station_dict[s])
                else:
                    miss_stations.append(s)
                    continue
            j += 1
            if j % 20 == 0:
                print("Searching {} stations...".format(j))
        df = pd.DataFrame.from_dict(new_stations, orient='index').reset_index()
        df.rename(index=str, columns={"index": "station"}, inplace=True)
        try:
            df.to_sql('geo_stations', con=self.engine, if_exists='append',
                      index=False)
            print("Wrote {} out of {} new station locations to database"
                  .format(j, len(new_set)))
        except Exception as e:
            print(e)
            raise
        return miss_stations

    def daily_station_summary(self, start, end, geo=True,
                              create=True, table=''):
        """Try not to use on entire data set, might have memory limits"""
        self.auth_table()
        self.pd_create_engine()
        df_nk = pd.read_sql_query("select remote, booth, station from "
                                  "name_keys;", con=self.engine)
        if notEmptyStr(start) and notEmptyStr(end):
            QUERY = ("select * from turnstiles where date >= '{}' and "
                     "date <= '{}' and description = 'REGULAR';"
                     .format(start, end))
        elif notEmptyStr(start):
            QUERY = ("select * from turnstiles where date >= '{}' and "
                     "description = 'REGULAR';".format(start))
        elif notEmptyStr(end):
            QUERY = ("select * from turnstiles where date <= '{}' and "
                     "description = 'REGULAR';".format(end))
        else:
            QUERY = "select * from turnstiles where description = 'REGULAR';"

        df = pd.read_sql_query(QUERY, con=self.engine)

        # de-cumulate entry/exit numbers
        df['datime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df.drop(['date', 'time', 'description'], axis=1, inplace=True)
        df.drop_duplicates(keep='first', inplace=True)
        level = ['remote', 'booth', 'scp']
        df = df.sort_values(by=level + ['datime']).reset_index(drop=True)
        df['entry_diff'] = df.groupby(level)['entries'].diff()
        df['exit_diff'] = df.groupby(level)['exits'].diff()
        # might have negative values due to reasons ie. counter reset, etc.
        # but there shouldn't be negative entries/exits, set to zero
        df.loc[df['entry_diff'] < 0, 'entry_diff'] = 0
        df.loc[df['exit_diff'] < 0, 'exit_diff'] = 0
        df['date'] = df['datime'].dt.date
        df.drop(['scp', 'entries', 'exits', 'datime'], axis=1, inplace=True)

        # get station name from name_keys table
        df = df.merge(df_nk, how='inner', left_on=['booth', 'remote'],
                      right_on=['booth', 'remote']).fillna(0)
        df = df.groupby(['station', 'date'])['entry_diff',
                                             'exit_diff'].sum().reset_index()
        if geo:
            df_geo = pd.read_sql_query("select * from geo_stations;",
                                       con=self.engine)
            df = df.merge(df_geo, how='inner', on='station')
        if create and notEmptyStr(table):
            try:
                df.to_sql(table, con=self.engine, if_exists='replace',
                          index=True)
                print("Table {} created.".format(table))
            except Exception as e:
                print(e)
                raise
        return df
