''' Data loader utility. '''

import pyodbc

from typing import List, Dict, Tuple

from datautility.util.object import merge
from datautility.constants import queries
from datautility.util.logging import get_logger
from datautility.util import db, fs, string, types, csv
from datautility.util.data.comparator import compare_tables
from datautility.util.data.exceptions import ValidationError
from datautility.util.data.sqlgen import gen_sync_sql, get_insert_generator, gen_comparator_query, gen_merge_sql

SQL_DATETIME_TYPE = 93
SQL_DATETIMEOFFSET_TYPE = -155

DEFAULT_PARAMS = {
    'batch_size': 1,
    'incremental': False,
    'identifier_column': 'id',
    'import_column': 'updated_at',
    'updated_column': 'updated_at'
}

logger = get_logger('data:loader')


def sync_with_data(target_db, data, **params):
    ''' Synchronize the database table using the provided data. '''
    table = params['table']

    validate(data)

    # Data types of each cells in the data will be converted if type converters are provided in the params.
    if params.get('types'):
        data = convert_data_types(
            data,
            params.get('types'),
            has_header=True
        )

    connection = db.connect(**target_db)

    logger.info('Syncing - {}.{}'.format(target_db['schema'], table))

    sql = gen_sync_sql(data, **params)

    cursor = connection.cursor()

    cursor.execute(sql)
    cursor.commit()

    logger.info('Synced - {}.{}'.format(target_db['schema'], table))

    cursor.close()


def sync_from_csv(target_db, src, **params):
    '''
    Synchronize the database using a CSV file as a source data.

    :param `target` Dictionary of connection details for destination database.
    :type dict

    :param `src` Source path of the csv file.
    :type string

    :param `table` Table name in which data is to be transfered.
    :type string

    :param `types` List of callable function each specifying types to apply to corresponding column.
    :type list

    :param `identity_insert` Boolean value to specify if identity insert should be set or not.
    :type boolean

    :param `comparators` List of identifiers by which tables are compared.
    :type list
    '''
    data = csv.load(src)
    sync_with_data(target_db, data, **params)


def transfer(source, target, **params):
    '''
    Transfer data to given table using provided sql query
    and connection params.

    :param `sql` Query to be used in source DB as data source.
    :type string
    :param `table` Target table to load data into. (This table must already exist in destination database.)
    :type string
    :param `source` Dictionary of connection details for source database.
    :type dict
    :param `target` Dictionary of connection details for destination database.
    :type dict
    :param `batch_size` Number of rows to be fetched in a single batch.
    :type number
    :param `pre_sql` SQL query or path to a sql file that needs to be executed before data insert.
    :type string
    :param `post_sql` SQL query or path to a sql file that needs to be executed after data insert.
    :type string
    :param `incremental` Boolean that specifies whether or not to transfer only incremental data. Defaults to `False`
    :type bool
    :param `set_identity` Boolean that specifies whether or not to enable identity insert for the target table. Defaults to `False`
    :type bool
    :param `import_column` Column in target table that can be
        compared with `updated_column` to identify which rows need
        to be imported again. Defaults to 'updated_at'.
    :type string
    :param `identifier_column` Source table identifier column that
        represents a unique row. Defaults to 'id'.
    :type string
    :param `updated_column` Source table column that denotes a change
        in a previously fetched row. Defaults to 'updated_at'.
    :type string
    '''
    # Merge default params with provided params.
    params = merge(DEFAULT_PARAMS, params)

    sql = params['sql']
    table = params['table']
    batch_size = params['batch_size']

    pre_hook = params.get('pre_sql')
    post_hook = params.get('post_sql')
    incremental = params.get('incremental')

    source_conn = db.connect(**source)
    dest_conn = db.connect(**target)

    if target.get('driver') != db.PG_DRIVER:
        dest_conn.add_output_converter(SQL_DATETIME_TYPE, db.handle_datetime)
        dest_conn.add_output_converter(
            SQL_DATETIMEOFFSET_TYPE, db.handle_datetimeoffset
        )

    if source.get('driver') != db.PG_DRIVER:
        source_conn.add_output_converter(SQL_DATETIME_TYPE, db.handle_datetime)
        source_conn.add_output_converter(
            SQL_DATETIMEOFFSET_TYPE, db.handle_datetimeoffset
        )

    logger.debug('Using source connection to host: {host}'.format(
        host=source['host'])
    )
    src_cursor = source_conn.cursor()

    logger.debug('Using target connection to host: {host}'.format(
        host=target['host'])
    )
    dest_cursor = dest_conn.cursor()

    if incremental:
        dest_cursor.execute(queries.MAX_VALUE.format(
            column=params['import_column'],
            table=table
        ))

        max_value = dest_cursor.fetchone().max_value

        src_cursor.execute(queries.UPDATED_ROW_IDENTIFIERS.format(
            source='({})'.format(sql.replace(';', '')),
            value=max_value or '',
            identifier=params['identifier_column'],
            updated_column=params['updated_column']
        ))

        rows = src_cursor.fetchall()

        ids_updated = [str(x[0]) for x in rows]

        dest_cursor.execute(queries.DELETE_UPDATED_ROWS.format(
            target=table,
            value_list=', '.join(ids_updated) or 'NULL',
            identifier=params['identifier_column']
        ))

        sql = queries.FILTER_ROWS.format(
            sql=sql.replace(';', ''),
            updated_column=params['updated_column'],
            value=max_value or ''
        )

        logger.info('Total rows deleted: {}\n'.format(dest_cursor.rowcount))

    # Set the number of rows to be fetched in a single batch.
    src_cursor.arraysize = batch_size

    logger.debug('Executing sql in source db: {}\n'.format(source['schema']))
    src_cursor.execute(sql)

    if target.get('driver') != db.PG_DRIVER:
        generate_insert = get_insert_generator(table, src_cursor.description)
    else:
        generate_insert = get_insert_generator(
            table, src_cursor.description, False
        )

    # If `pre_sql` is an sql file that exists, resolve the
    # file's contents else use it as an inline sql query.
    if pre_hook:
        pre_sql = fs.read(pre_hook) if fs.exists(pre_hook) else pre_hook

        logger.info('Running pre script on destination database: {sql}'.format(
            sql=pre_sql)
        )
        dest_cursor.execute(pre_sql)

    if target.get('driver') != db.PG_DRIVER:
        dest_cursor.execute('SET QUOTED_IDENTIFIER OFF;')
        if params.get('set_identity'):
            dest_cursor.execute(queries.IDENTITY_INSERT_ON.format(table=table))

    logger.info('Inserting data into target table: [{db}].{table}'.format(
        db=target['schema'], table=table)
    )

    rows = src_cursor.fetchmany()
    total_rowcount = 0

    while rows:
        # Create insert query using generate_insert and fetched rows.
        query = generate_insert(rows)

        dest_cursor.execute(query)
        total_rowcount += dest_cursor.rowcount

        rows = src_cursor.fetchmany()

    if target.get('driver') != db.PG_DRIVER:
        dest_cursor.execute('SET QUOTED_IDENTIFIER ON;')

        if params.get('set_identity'):
            dest_cursor.execute(
                queries.IDENTITY_INSERT_OFF.format(table=table)
            )

    logger.info('Total rows inserted: {rowcount}\n'.format(
        rowcount=total_rowcount)
    )

    # If `post_sql` is an sql file that exists, resolve the
    # file's contents else use it as an inline sql query.
    if post_hook:
        post_sql = fs.read(post_hook) if fs.exists(post_hook) else post_hook

        logger.info(
            'Running post script on destination database: {sql}'.format(
                sql=post_sql
            )
        )
        dest_cursor.execute(post_sql)

    dest_cursor.commit()
    src_cursor.close()
    dest_cursor.close()


def convert_data_types(data, types, has_header=True):
    '''
    Returns a new multidimentional list applying types to each column
    based on a list of python callables each specifying the types
    to apply to corresponding column.

    :param data Multidimentional list to be in which types need to be converted.
    :type list

    :param types List of callable each specifying types to apply to corresponding column.
    :type list

    :param has_header Boolean value to specify if the data has header or not.
    :type bool

    Example:
        data = [
            ['id', 'name', 'salary'],
            [1, 'Foo', '12000.50'],
            [2, 'Hello', '15000.25'],
        ]

        types = [int, lambda x: x, float]
    '''

    validate(data)
    header = data[0]
    column_count = len(header)

    if (len(types) != column_count):
        raise AssertionError(
            'Mismatch in number of column type definitions and number of columns in data.'
        )

    # If type converters are given in a dictionary,
    # convert it into a list by mapping it with header data.
    if isinstance(types, dict):
        types = get_types_list(header, types)

    rows = data[1:] if has_header else data

    def row_converter(row):
        return list(map(lambda type, value: type(value), types, row))

    converted_data = list(map(row_converter, rows))

    if has_header:
        return [header] + converted_data

    return converted_data


def validate(data):
    '''
    Checks if the number of columns in data is consistent.

    Suceeds if the data is valid, otherwise raises an exeption.
    '''
    # TODO: Also validate to ensure types for each columns are consistent.

    column_count = len(data[0])

    for idx, line in enumerate(data):
        if len(line) != column_count:
            raise AssertionError(
                'Number of columns mismatch in line number {}'.format(idx + 1)
            )


def get_types_list(header, types):
    '''
    Returns list of type converters mapping each columns
    in the header with dictionary of type converters.

    Example:
    header = ['id', 'name', 'date']
    types = {
        'name': string(),
        'id': integer(),
        'date': date()
    }
    result = [
        integer(),
        string(),
        date()
    ]
    '''
    try:
        return list(map(lambda column: types[column], header))
    except KeyError as ke:
        raise KeyError('Unable to locate {} column in type converters dictionary'.format(ke))


def validate_columns(source_columns: List[Tuple], target_columns: List[Tuple]):
    '''
    Validate the columns and their types during synchronization

    param `cursor` pyodbc connection cursor of database.
    type `Cursor`
    param `source_table` Name of source table name.
    type `string`
    param `target_table` Name of table name where data needs to be synchronized.
    type `string`

    Example:
        source_columns = [('id', 'int', 4), ('title', 'varchar', 100)]
        target_columns = [('id', 'int', 4), ('title', 'varchar', 100)]
    '''
    logger.info('Validating source and target tables\' columns.')
    diff_columns = list(set(source_columns) - set(target_columns))

    if diff_columns:
        logger.debug('Columns difference between source and target table: {}'.format(diff_columns))
        columns, data_types, buffer_lengths = zip(*diff_columns)
        message = 'The source columns `{}` doesn\'t match or is not compatible in target table'.format(columns)

        raise ValidationError(message)

    logger.info('Column validation passed.')


def synchronize(cursor, **params):
    '''
    Synchronize data of source_table to target_table.

    param `cursor` pyodbc connection cursor of database.
    type `Cursor`
    param `**params` Synchronize configuration.
    type `**kwargs`
        Keyword Arguments:
        param `source_table` Name of source table name.
        type `string`
        param `target_table` Name of table name where data needs to be synchronized.
        type `string`
        param `diff` Conditional flag to enable/disable checksum comparision.
        type `bool`
        param `comparators` List of comparision keys.
        type `list`
        param `identity_insert` Set identity insert ON or OFF.
        type `bool`
        param `dry_run` It logs the query instead of executing.
        type `bool`
        param `should_validate` Conditional flag to enable/disable validation.
        type `bool`
    '''
    # Resolve params
    source_table = params.get('source_table')
    target_table = params.get('target_table')
    dry_run = params.get('dry_run') or False
    should_validate = params.get('should_validate') != False

    logger.info('Synchronizing `{}` -> `{}`.'.format(source_table, target_table))

    source_columns = db.get_columns(cursor, source_table)
    target_columns = db.get_columns(cursor, target_table)
    columns, _, _ = zip(*source_columns)

    if should_validate:
        skip_sync = compare_tables(cursor, source_table, target_table, columns)

        if not dry_run and skip_sync:
            logger.info('Skipping synchronization for table: `{}`\n'.format(target_table))

            return

        validate_columns(source_columns, target_columns)

    sync_query = gen_merge_sql(keys=columns, **params)

    if dry_run:
        logger.info('Generated synchronize SQL: \n{}'.format(sync_query))

        return

    cursor.execute(sync_query)

    logger.info('Total row(s) affected: {}\n'.format(cursor.rowcount))


def bulk_synchronize(connection: pyodbc.Connection, data: List[Dict], dry_run: bool = False):
    '''
    Synchronize the data with connection.

    param `connection` pyodbc connection of database.
    type `Connection`
    param `data` List of dictionary defining synchronization config.
    type `list`
    param `dry_run` It logs the query instead of executing.
    type `bool`

    Example:
    data = [
        { 'source_table': 'etl.xwalk_subjects', 'target_table': 'dbo.xwalk_subjects'},
        { 'source_table': 'etl.xwalk_timezones', 'target_table': 'dbo.xwalk_timezones' }
    ]
    '''
    logger.info('Starting database connection.')
    cursor = connection.cursor()
    logger.info('Database connection established.')
    logger.info('Started synchronizing tables.\n')

    for row in data:
        synchronize(cursor, dry_run=dry_run, **row)

    logger.info('Committing transaction.')
    connection.commit()
    logger.info('Synchronization completed.')
    cursor.close()
