''' Type converter utilities '''

import datetime

from datautility.util.decorators import noneable


@noneable
def timestamp(x):
    '''
    Returns datetime.datetime object of given datetime string.
    :params x String in format YYYY-mm-dd HH:MM:SS or YYYY-mm-dd HH:MM:SS.fffff
    :type str
    '''
    if len(x) == 19:
        return datetime.datetime.strptime(x[:19], '%Y-%m-%d %H:%M:%S')

    return datetime.datetime.strptime(x[:25], '%Y-%m-%d %H:%M:%S.%f')


@noneable
def date(x):
    '''
    Returns datetime.date object of given date string.
    :params x String in format YYYY-mm-dd
    :type str
    '''
    return datetime.date(int(x[0:4]), int(x[5:7]), int(x[8:10]))


@noneable
def time(x):
    '''
    Returns time object of given date string.
    :params x String in format HH:MM:SS
    :type str
    '''
    return datetime.time(hour=int(x[0:2]), minute=int(x[3:5]), second=int(x[6:8]))


@noneable
def integer(x):
    '''
    Returns int object eqivalent to given data.
    '''
    return int(x)


@noneable
def floating(x):
    '''
    Returns float object eqivalent to given data.
    '''
    return float(x)


@noneable
def string(x):
    '''
    Returns str object eqivalent to given data.
    '''
    return str(x)


@noneable
def boolean(x):
    '''
    Returns bool object eqivalent to given data.
    '''
    if x == '0' or x.lower() == 'false':
        return False

    return bool(x)
