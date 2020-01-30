''' Database Utility Functions. '''

import os
import pyodbc
import struct
import logging

from tempfile import mkstemp
from typing import List, Tuple
from datetime import datetime, timedelta

from datautility.util.types import is_string, is_iterable

PG_DRIVER = '{PostgreSQL Unicode}'
ODBC_DRIVER = '{ODBC Driver 17 for SQL Server}'
CONNECTION_STRING = 'DRIVER={driver};SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password}'


def connect(**params):
    ''' Open connection to a SQL Server Database. '''
    connection_str = CONNECTION_STRING.format(
        driver=params.get('driver') or ODBC_DRIVER,
        server=params['host'],
        database=params['schema'],
        username=params['login'],
        password=params['password'],
        port=params.get('port') or 1433
    )

    return pyodbc.connect(connection_str)


def handle_datetimeoffset(dto_value):
    '''
    Convert datetimeoffset byte value to datetime object and return its string representation.
    '''
    # ref: https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
    tup = struct.unpack('<6hI2h', dto_value)

    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:09d} {:+03d}:{:02d}".format(*tup)


def handle_datetime(dt_value):
    '''
    Convert datetime byte value to datetime object and return its string representation.
    '''
    tup = struct.unpack('<2l', dt_value)
    date_time = datetime(1900, 1, 1) + timedelta(days=tup[0], seconds=round(tup[1] / 300, 3))

    return date_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:23]


def get_columns(cursor: pyodbc.Cursor, table_name: str) -> List[Tuple[str]]:
    '''
    Return the columns of the given table.

    param `cursor` pyodbc connection cursor of database.
    type `Cursor`
    param `table_name` Name of the table.
    type `string`

    Example:
        Response = [('id', 'int', 4), ('name', 'varchar', 100), ('description', 'varchar', 2000)]
    '''
    try:
        schema, table = table_name.split('.')[-2:]
    except:
        schema, table = None, table_name

    columns = [(row.column_name, row.type_name, row.buffer_length) for row in cursor.columns(schema=schema, table=table)]
    logging.debug('Table `{}` columns: `{}`'.format(table_name, columns))

    return columns
