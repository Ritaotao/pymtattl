# Download mta turnstile data files from 2018-01-01 to 2018-02-01,
# write into postgres database (please make sure mta_sample already exists)

from pymtattl import Downloader, Cleaner, station_mapping

download = Downloader(directory='./data/', local=True)
paths = download.run(date_range=("2019-01-01", "2019-02-01"), verbose=10)
print("Show first 3 paths: {}".format(paths[:3]))
# dbstring: database urls used by sqlalchemy;
# for databases other than sqlite, please make sure database already exists
# dialect+driver://username:password@host:port/database
# postgres: 'postgresql://scott:tiger@localhost/mydatabase'
# mysql: 'mysql://scott:tiger@localhost/foo'
# sqlite: 'sqlite:///foo.db'
# (more info could found here: https://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql)
#clean = Cleaner(input_path=data_path,
#                dbstring='postgresql://user:p@ssword@localhost:5432/mta_sample')
#clean.run()

#station_mapping(file_path='data/Remote-Booth-Station.xlsx',
#                dbstring='postgresql://user:p@ssword@localhost:5432/mta_sample2')
print("Example complete.")