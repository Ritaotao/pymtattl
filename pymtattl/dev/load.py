import pandas as pd
from urllib.request import urlopen


def read_prior(url):
    """
        Parse mta turnstile data prior to 2014-10-18
        drop scp
        return pandas dataframe
    """
    data = urlopen(url)
    rows = []
    err_count = 0
    for line in data:
        cols = line.strip().split(',')
        ncol = len(cols)
        if (ncol - 3) % 5 > 0:
            err_count += 1
        else:
            # keys: ca/units
            # + every 5 add: daten/timen/entriesn/exitsn
            keys = cols[:2]
            for i in range(3, ncol, 5):
                row = tuple(keys+cols[i:i+2]+cols[i+3:i+5])
                rows.append(row)
    labels = ['booth', 'remote', 'date', 'time', 'entries', 'exits']
    df = pd.DataFrame.from_records(rows, columns=labels)
    print("%d invalid lines with wrong # columns" % err_count)
    return df


def read_current(url):
    """
        Parse mta turnstile data current (>= 2014-10-18)
        drop scp, station, linename, division
        return pandas dataframe
    """
    labels = ['booth', 'remote', 'scp', 'station', 'linename', 'division',
              'date', 'time', 'description', 'entries', 'exits']
    use = [0, 1, 6, 7, 9, 10]
    df = pd.read_table(url, sep=',', header=0, names=labels, usecols=use)
    return df


def load_tables(init_tables=True):
    """
        Load or init index table and latest table,
        index table (primary key for data table):
            id, booth, remote;
        latest table (as base numbers for following data file):
            index_id, date_time, raw_entries, raw_exits;
        init_tables: boolean or list,
            list of index table and latest table paths,
            default to true, create init_tables
    """
    id_cols = ['booth', 'remote']
    lt_cols = ['index_id', 'last_time', 'raw_entries', 'raw_exits']
    if isinstance(init_tables, list) and len(init_tables) == 2:
        index_df = pd.read_table(init_tables[0], sep=',', index_col=0,
                                 header=0, names=id_cols)
        latest_df = pd.read_table(init_tables[1], sep=',', names=lt_cols)
    elif init_tables is True:
        index_df = pd.DataFrame(columns=id_cols)
        latest_df = pd.DataFrame(columns=lt_cols)
    else:
        raise Exception("Please provide init_tables")
    return (index_df, latest_df)


def update_index(df, index_table):
    """
        update index table with new booth, remote pairs,
        drop booth, remote columns, keep only index column from df
    """
    id_pairs = df[['booth', 'remote']].drop_duplicates()
    index = index_table.append(id_pairs, ignore_index=True).drop_duplicates()
    df = df.merge(index.reset_index(), on=['booth', 'remote'], how='left')
    df.drop(['booth', 'remote'], axis=1, inplace=True)
    df.rename(columns={'index': 'index_id'}, inplace=True)
    return (df, index_table)


def update_latest(df, latest_table):
    """
        update latest table with most recent raw numbers found in df
        for each index (booth, remote pairs)
    """
    keys = ['index_id', 'date_time']
    vals = ['entries', 'exits']
    df = df.groupby(keys)['entries', 'exits'].sum().reset_index()
    firsts = df.groupby('index_id').nth(0).reset_index()
    lasts = df.groupby('index_id').nth(-1).reset_index()
    firsts = firsts.merge(latest_table, on='index_id', how='left').fillna(0)
    # might yield negative value due to reset/rollback
    # will be clipped after diffed
    firsts['entries'] = (firsts['entries'] - firsts['raw_entries'])
    firsts['exits'] = (firsts['exits'] - firsts['raw_exits'])
    firsts = firsts.set_index(keys, inplace=True)[vals]

    df = df.set_index(keys).groupby(level=0).diff()
    df = df.fillna(firsts).reset_index()
    df = df.fillna(0).clip(0)

    df = df.merge(latest_table, on='index_id', how='left')


def read_url(url, index_table, latest_table):
    if utils.getPubDate(url) < 141018:
        df = read_prior(url)
    else:
        df = read_current(url)
    print("Load into dataframe, start processing...")

    # convert to date_time and integer columns
    df['date'] = df['date'].apply(utils.formatDate)
    df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df.drop(['date', 'time'], axis=1, inplace=True)
    df['entries', 'exits'] = df['entries', 'exits'].applymap(int)

    df, index_df = update_index(df, index_table)
