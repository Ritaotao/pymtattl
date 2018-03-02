import os
from datetime import datetime


def getPubDate(fname):
    """
    Return the date (yymmdd) from the data url or file name
    """
    return int(fname.split('_')[1].split('.')[0])


def createFolder(root='Current', branch=None):
    """
    Create new folder under given directory,
    return created directory as a string.
    params:
        root (string): path to create new folder under
        branch (string): name of new folder
    """
    if root == 'Current':
        work_dir = os.getcwd()
    elif os.path.exists(root):
        work_dir = root
    else:
        raise OSError('Error: Directory does not exist: {}'.format(root))

    if branch is not None:
        work_dir = os.path.join(work_dir, str(branch))
        if not os.path.exists(work_dir):
            try:
                os.mkdir(work_dir)
                print("Created folder {}.".format(work_dir))
            except OSError as err:
                raise OSError('Error: Failed to create dir {}: {}'
                              .format(work_dir, err))
    return work_dir


def writeDB(filename, data, c):
    """
    Parse mta turnstile data differently,
    due to changes made post 2014-10-18,
    return sqlite cursor object.
    params:
        filename (string): use to determine time period
        data (list): list of row strings within file
        c: a sqlite cursor object
    """
    if int(filename.split('_')[1].split('.')[0]) < 141018:
        for line in data:
            cols = line.strip().split(',')
            if (len(cols) - 3) % 5 == 0:
                keys, vals = cols[:3], cols[3:]
                # keys: ca/units/scp
                # + every 5: daten/timen/descn, entriesn/exitsn
                for i in range(0, len(vals), 5):
                    rows = [tuple(keys
                                  + [datetime.strptime(vals[i], '%m-%d-%y')
                                     .strftime('%Y-%m-%d')]
                                  + vals[(i+1):(i+3)]
                                  + [int(j) for j in vals[(i+3):(i+5)]])]
                c.executemany('INSERT INTO turnstiles VALUES '
                              '(?,?,?,?,?,?,?,?)', rows)
    else:
        for line in data[1:]:
            cols = line.strip().split(',')
            if len(cols) == 11:
                row = tuple(cols[:3]
                            + [datetime.strptime(cols[6], '%m/%d/%Y')
                               .strftime('%Y-%m-%d')]
                            + cols[7:9]
                            + [int(i) for i in cols[9:11]])
                c.execute('INSERT INTO turnstiles VALUES '
                          '(?,?,?,?,?,?,?,?)', row)
    return c
