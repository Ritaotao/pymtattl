from datetime import datetime


def getPubDate(name):
    """
    Return the date (yymmdd) from the data url or file name
    """
    return int(name.split('_')[1].split('.')[0])


def formatDate(val):
    """
    Parse date string to '%Y-%m-%d'
    """
    for fmt in ('%m-%d-%y', '%m/%d/%Y'):
        try:
            date = datetime.strptime(val, fmt).strftime('%Y-%m-%d')
            return date
        except ValueError as e:
            pass
    raise ValueError('no valid date format found.')
