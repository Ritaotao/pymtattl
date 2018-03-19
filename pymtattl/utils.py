import os
from datetime import datetime


def getPubDate(fname):
    """
    Return the date (yymmdd) from the data url or file name
    """
    return int(fname.split('_')[1].split('.')[0])


def formatDate(val):
    """
    String '%m-%d-%y' to string'%Y-%m-%d'
    """
    return datetime.strptime(val, '%m-%d-%y').strftime('%Y-%m-%d')


def notEmptyStr(var):
    """
    Check whether a variable is a non-empty string
    """
    return bool(isinstance(var, str) and var.strip() != '')


def strParms(parms):
    stringParms = ""
    for k, v in parms.items():
        stringParms += "{}={} ".format(k, str(v))
    return stringParms.strip()


def dbInsert(table, cols, vals, dbtype):
    """
    Handles db insert statement difference here
    """
    if dbtype == 'sqlite':
        vals = ','.join(['?'] * (len(vals.split(','))+1))
        iq = 'INSERT INTO ' + str(table) + ' VALUES (' + vals + ');'
    elif dbtype == 'postgres':
        cols = ' (' + ','.join(cols) + ') '
        iq = 'INSERT INTO ' + str(table) + cols + ' VALUES (' + vals + ');'
    return iq


def dbPK(dbtype):
    if dbtype == 'postgres':
        pk = 'id SERIAL PRIMARY KEY'
    elif dbtype == 'sqlite':
        pk = 'id INTEGER PRIMARY KEY'
    return pk


def createFolder(root, branch=None):
    """
    Create new folder under given directory,
    return created directory as a string.
    params:
        root (string): path to create new folder under
        branch (string): name of new folder
    """
    if os.path.isdir(root):
        work_dir = os.path.abspath(root)
    elif root == '':
        work_dir = os.getcwd()
    else:
        print("Couldn't find directory: ", root)
        work_dir = os.getcwd()
        folder = os.path.basename(root)
        assert notEmptyStr(folder), "Couldn't parse folder name."
        create = input("Create folder <{}> under <{}>?(y/n)"
                       .format(folder, work_dir)).lower()
        if create == 'y':
            return createFolder(root=work_dir, branch=folder)
        else:
            raise OSError('Error: Directory does not exist: {}'.format(root))

    if notEmptyStr(branch):
        work_dir = os.path.join(work_dir, branch)
        if not os.path.isdir(work_dir):
            try:
                os.mkdir(work_dir)
                print("Created {}.".format(work_dir))
            except OSError as err:
                raise OSError('Error: Failed to create dir {}: {}'
                              .format(work_dir, err))
        else:
            print("{} alreayd exists.".format(work_dir))
    return work_dir


def writeDB(dbtype, filename, data, c):
    """
    Parse mta turnstile data differently,
    due to changes made post 2014-10-18,
    return db cursor object.
    params:
        filename (string): use to determine time period
        data (list): list of row strings within file
        c: a db cursor object
    """
    cols = ['booth', 'remote', 'scp', 'date', 'time', 'description',
            'entries', 'exits']
    iq_tn = dbInsert('turnstiles', cols, '%s,%s,%s,%s,%s,%s,%s,%s', dbtype)
    if int(filename.split('_')[1].split('.')[0]) < 141018:
        for line in data:
            line = line.replace('\x00', '')
            cols = line.strip().split(',')
            if (len(cols) - 3) % 5 == 0:
                keys, vals = cols[:3], cols[3:]
                # keys: ca/units/scp
                # + every 5: daten/timen/descn, entriesn/exitsn
                rows = []
                for i in range(0, len(vals), 5):
                    try:
                        row = (keys + [formatDate(vals[i])] + vals[(i+1):(i+3)]
                               + [int(j) for j in vals[(i+3):(i+5)]])
                        if dbtype == 'sqlite':
                            row = tuple([None] + row)
                        else:
                            row = tuple(row)
                        rows.append(row)
                    except ValueError as e:
                        continue
                if rows:
                    try:
                        c.executemany(iq_tn, rows)
                    except Exception as e:
                        print(e)
                        print(filename)
                        print(rows)
                        raise
    else:
        for line in data[1:]:
            line = line.replace('\x00', '')
            cols = line.strip().split(',')
            if len(cols) == 11:
                try:
                    row = (cols[:3] + [formatDate(cols[6])] + cols[7:9]
                           + [int(i) for i in cols[9:11]])
                    if dbtype == 'sqlite':
                        row = tuple([None] + row)
                    else:
                        row = tuple(row)
                    c.execute(iq_tn, row)
                except ValueError as e:
                    continue
    return c
