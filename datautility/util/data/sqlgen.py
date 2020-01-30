''' SQL Generator utility. '''

from typing import List, Dict

from datautility.util.types import is_dict, is_list
from datautility.util import db, fs, string, types, logging
from datautility.constants.common import SQL_DELETE, SQL_UPDATE, SQL_INSERT
from datautility.constants.queries import (
    INSERT_STATEMENT,
    VALUES_CONSTRUCTOR,
    IDENTITY_INSERT_ON,
    MERGE_WHEN_MATCHED,
    IDENTITY_INSERT_OFF,
    MERGE_INSERT_STATEMENT,
    MERGE_UPDATE_STATEMENT,
    MERGE_DELETE_STATEMENT,
    MERGE_STATEMENT_USING_TABLE,
    MERGE_STATEMENT_USING_VALUES,
    MERGE_WHEN_NOT_MATCHED_BY_TARGET,
    MERGE_WHEN_NOT_MATCHED_BY_SOURCE
)

SQL_REPLACE_MAP = {
    'True': '1',
    'False': '0',
    '\\\\': '\\',  # remove forward slashes added by pyodbc
    'None': 'NULL'
}

logger = logging.get_logger('data:sqlgen')


def gen_values_sql(rows):
    '''
    Generate VALUES constructor sql.

    Note: currently supports only MSSQL.
    '''
    if not rows:
        return None

    sql = VALUES_CONSTRUCTOR.format(
        values=', '.join([string.format_string('N', row) for row in rows])
    )

    # Use replace map on SQL query.
    return string.apply_replace_map(sql, SQL_REPLACE_MAP)


def gen_merge_sql(**params):
    '''
    Generate merge sql statement from the given params.

    Note: currently supports only MSSQL.

    param `**params` Synchronize configuration.
    type `**kwargs`
        Keyword Arguments:
        param `source_table` Name of source table name.
        type `string`
        param `target_table` Name of table name where data needs to be synchronized.
        type `string`
        param `keys` Sets of keys i.e. column name.
        type `list`
        param `rows` Rows that's needs to be synchronized.
        type `list[dict]`
        param `statement` Merge statement that's need to be executed.
        type `string`
        param `diff` A diff flag to enable/disable checksum comparision.
        type `bool`
        param `comparators` List or dictionary of comparision keys.
        type `list` or `dict`
        param `identity_insert` Set identity insert ON or OFF.
        type `bool`
        param `custom_params`
        type `dict`
            Custom Params Keys:
            param `when_matched` A dict of param for when matched condition.
            type `dict` or `bool`
            param `when_not_matched_by_source` A dict of param for when not matched by source condition.
            type `dict` or `bool`
            param `when_not_matched_by_target` A dict of param for when not matched by target condition.
            type `dict` or `bool`
    '''
    # Resolve params
    source_table = params.get('source_table')
    target_table = params.get('target_table')
    keys = params.get('keys') or []
    diff = params.get('diff')
    rows = params.get('rows')
    statement = params.get('statement')
    excluded_keys = params.get('exclude') or []
    comparators = params.get('comparators')
    identity_insert = params.get('identity_insert')
    custom_params = params.get('custom_params')
    statement = statement or MERGE_STATEMENT_USING_TABLE

    if not custom_params and not keys:
        raise KeyError('`keys` must be provided in params.')

    if not comparators and 'id' in keys:
        comparators = ['id']

    if is_list(comparators):
        comparators = zip_params(comparators)

    keys = [x for x in keys if x not in excluded_keys]
    update_keys = [x for x in keys if 's.{}'.format(x) not in comparators.values()]

    statement += merge_and_gen_stmt(
        diff=diff,
        insert_keys=keys,
        update_keys=update_keys,
        custom_params=custom_params
    )

    columns = ', '.join(keys)
    compare_expr = ' AND '.join([
        '{} = {}'.format(target, source) for target, source in comparators.items()
    ])
    values_cstr = gen_values_sql(rows)

    sql = ''
    indent = 4 * ' '  # Just to make the output look sort of cool - LOL ;)!

    if identity_insert:
        sql += indent + IDENTITY_INSERT_ON.format(table=target_table)

    statement += ';\n'

    sql += statement.format(
        columns=columns,
        values_cstr=values_cstr,
        source_table=source_table,
        target_table=target_table,
        compare_expr=compare_expr
    )

    if identity_insert:
        sql += indent + IDENTITY_INSERT_OFF.format(table=target_table)

    return sql


def get_merge_params(**params) -> Dict:
    '''
    Generate default merge statement.

    params `**params`
    type `kwargs`
        Keyword Arguments:
        param `diff` A diff flag to enable/disable checksum comparision.
        type `bool`
        param `insert_keys` A list of insert column keys.
        type `list`
        param `update_keys`  A list of update column keys.
        type `list`
    '''
    diff = params.get('diff')
    insert_params = zip_insert_params(params.get('insert_keys'))
    update_params = zip_params(params.get('update_keys'))

    return {
        'when_not_matched_by_target': {
            'statement_type': SQL_INSERT,
            'params': insert_params,
        },
        'when_matched': {
            'statement_type': SQL_UPDATE,
            'params': update_params,
            'checksum_params': update_params if diff else None
        },
        'when_not_matched_by_source': {
            'statement_type': SQL_DELETE
        }
    }


def gen_sync_sql(data, **params):
    '''
    Generate sql statement for the provided data. Data structure
    expected is a list of rows, the first row being the list of
    column names.

    Note: currently supports only MSSQL.

    Example:
        data = [
            ['id', 'name', 'description'],
            [1, 'Foo', 'Bar'],
            [2, 'Hello', 'World'],
        ]
    '''
    # Resolve keys and rows
    keys = data[0]
    rows = data[1:]

    return gen_merge_sql(rows=rows, keys=keys, statement=MERGE_STATEMENT_USING_VALUES, target_table=params.get('table'), **params)


def get_insert_generator(table, description, format_string=True):
    '''
    Get an insert generator using given cursor description
    and target table name.
    TODO: Write tests.
    '''
    # Append column name to column list from description tuple.
    column_list = [x[0] for x in description]

    def generate_insert_query(rows):
        '''
        Generate an insert query by adding rows to insert format.
        '''
        fchar = 'N' if format_string else ''
        query = INSERT_STATEMENT.format(
            table=table,
            columns=', '.join(column_list),
            values=', '.join(string.format_string(fchar, row) for row in rows)
        )

        # Use replace map on SQL query.
        transformed_query = string.apply_replace_map(query, SQL_REPLACE_MAP)

        return transformed_query

    return generate_insert_query


def get_merge_condition(keys: Dict) -> str:
    ''' Generate condition expression for the merge query. '''
    return ' AND ' + gen_comparator_query(keys.keys()) + ' <> ' + gen_comparator_query(keys.values())


def gen_comparator_query(keys: List) -> str:
    '''
    Generate checksum query.

    Example:
        keys:  `['name', 'description']`

        Result: `CHECKSUM(name, description)`
    '''
    return 'CHECKSUM(' + ', '.join([x for x in keys]) + ')'


def zip_params(keys: List) -> Dict:
    ''' Generate zipped update params. '''
    source_keys = map(lambda x: 's.{}'.format(x), keys)
    target_keys = map(lambda x: 't.{}'.format(x), keys)

    return dict(zip(target_keys, source_keys))


def zip_insert_params(keys: List) -> Dict:
    ''' Generate zipped insert params. '''
    source_keys = map(lambda x: 's.{}'.format(x), keys)

    return dict(zip(keys, source_keys))


def gen_merge_insert_statement(keys: Dict) -> str:
    ''' Generate merge insert statement. '''
    columns = ', '.join(keys.keys())
    columns_pfx = ', '.join([x for x in keys.values()])

    return MERGE_INSERT_STATEMENT.format(columns=columns, columns_pfx=columns_pfx)


def gen_merge_update_statement(keys: Dict) -> str:
    ''' Generate merge update statement. '''
    update_expr = ', '.join([
        '{} = {}'.format(target_key, source_key) for target_key, source_key in keys.items()
    ])

    return MERGE_UPDATE_STATEMENT.format(update_expr=update_expr)


def get_stmt_by_condition(**params) -> str:
    '''
    Get statement by condition.

    params `**params`
    type `kwargs`
        Keyword Arguments:
        param `params` The params used to generate the statement.
        type `dist`
        param `condition_name` Type of conditional statement.
        type `str`
        param `statement_type` Type of statement: insert, update, delete.
        type `str`
        param `checksum_params` The checksum params to generate conditional statement.
        type `dict`
    '''
    stmt_params = params.get('params')
    condition_name = params.get('condition_name')
    statement_type = params.get('statement_type')
    checksum_params = params.get('checksum_params')
    expr = get_stmt_by_type(statement_type, stmt_params)

    if not expr:
        return ''

    if condition_name == 'when_matched':
        diff_expr = get_merge_condition(checksum_params) if checksum_params else ''

        return MERGE_WHEN_MATCHED.format(expr=expr, diff_expr=diff_expr)

    if condition_name == 'when_not_matched_by_source':
        return MERGE_WHEN_NOT_MATCHED_BY_SOURCE.format(expr=expr)

    if condition_name == 'when_not_matched_by_target':
        return MERGE_WHEN_NOT_MATCHED_BY_TARGET.format(expr=expr)

    return ''


def get_stmt_by_type(statement_type: str, condition_params: Dict) -> str or None:
    '''
    Get statement by it's type.

    param `statement_type` Type of statement: insert, update, delete.
    type `str`
    param `condition_params` The params used to generate the statement.
    type `dist`
    '''
    if statement_type == SQL_INSERT:
        return gen_merge_insert_statement(condition_params)

    if statement_type == SQL_UPDATE:
        return gen_merge_update_statement(condition_params)

    if statement_type == SQL_DELETE:
        return MERGE_DELETE_STATEMENT

    return None


def merge_and_gen_stmt(**params) -> str:
    '''
    Merge the custom params with default conditional params
    and generate statement.

    params `**params`
    type `kwargs`
        Keyword Arguments:
        param `diff` A diff flag to enable/disable checksum comparision.
        type `bool`
        param `insert_keys` A list of insert column keys.
        type `list`
        param `update_keys`  A list of update column keys.
        type `list`
        param `custom_params` The params used to generate the statement.
        type `dist`
    '''
    diff = params.get('diff')
    insert_keys = params.get('insert_keys')
    update_keys = params.get('update_keys')
    custom_merge_params = params.get('custom_params') or {}

    stmt = ''
    merge_params = get_merge_params(insert_keys=insert_keys, update_keys=update_keys, diff=diff)

    for condition_name, condition_params in merge_params.items():
        custom_condition_params = custom_merge_params.get(condition_name)

        if custom_condition_params == False:
            continue

        condition_params = custom_condition_params if is_dict(custom_condition_params) else condition_params

        stmt += get_stmt_by_condition(condition_name=condition_name, **condition_params)

    return stmt


def fetch_all(table: str, columns: List = None) -> str:
    ''' Generate an SQL query to fetch all rows from a provided table. '''
    columns_expr = '*' if not columns else ', '.join(columns)

    return 'SELECT {cols} FROM {table}'.format(cols=columns_expr, table=table)


def truncate(table: str) -> str:
    ''' Generate an SQL query to truncate a table. '''
    return 'TRUNCATE TABLE {table}'.format(table=table)
