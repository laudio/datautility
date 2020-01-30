''' Table diff and comparison utility. '''

import pyodbc

from typing import List

from datautility.util.logging import get_logger
from datautility.constants.queries import CHECKSUM_AGG_STATEMENT

logger = get_logger('data:comparator')

def compare_tables(cursor: pyodbc.Cursor, source_table: str, target_table: str, comparators: List = None) -> bool:
    '''
    Compare the checksum value of source_table and target_table.

    param `cursor` pyodbc connection cursor of database.
    type `Cursor`
    param `source_table` Name of source table name.
    type `string`
    param `target_table` Name of table name where data needs to be synchronized.
    type `string`
    param `comparators` List of columns which only needs to be compared.
    type `list`

    Example:
        source_table=etl.employees
        target_table=dbo.employees
        comparators=['id', 'first_name', 'last_name']
    '''
    columns =  ', '.join(comparators) if comparators else '*'

    logger.info('Getting the checksum value of given tables.')
    source_value = cursor.execute(CHECKSUM_AGG_STATEMENT.format(table=source_table, columns=columns)).fetchval()
    target_value = cursor.execute(CHECKSUM_AGG_STATEMENT.format(table=target_table, columns=columns)).fetchval()
    result = source_value == target_value
    logger.debug('Checksum value of table: {} = {}'.format(source_table, source_value))
    logger.debug('Checksum value of table: {} = {}'.format(target_table, target_value))
    logger.info('Checksum comparision status = {}'.format(result))

    return result
