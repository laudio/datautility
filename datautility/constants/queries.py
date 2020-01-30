''' Query Constants. '''

DELETE_UPDATED_ROWS = 'DELETE FROM {target} WHERE {identifier} IN ({value_list});'
FILTER_ROWS = '{sql} WHERE {updated_column} > \'{value}\';'
IDENTITY_INSERT_OFF = 'SET IDENTITY_INSERT {table} OFF;'
IDENTITY_INSERT_ON = 'SET IDENTITY_INSERT {table} ON;'
MAX_VALUE = 'SELECT MAX({column}) AS max_value FROM {table};'
UPDATED_ROW_IDENTIFIERS = 'SELECT {identifier} FROM {source} s WHERE s.{updated_column} > \'{value}\';'

INSERT_STATEMENT = 'INSERT INTO {table} ({columns}) VALUES {values};'

MERGE_STATEMENT_USING_TABLE = '''
    MERGE {target_table} t USING {source_table} s ON {compare_expr}'''

MERGE_STATEMENT_USING_VALUES = '''
    MERGE {target_table} t USING ({values_cstr}) AS s({columns}) ON {compare_expr}'''

MERGE_WHEN_MATCHED = '''
    WHEN MATCHED{diff_expr} THEN
        {expr}'''

MERGE_WHEN_NOT_MATCHED_BY_TARGET = '''
    WHEN NOT MATCHED BY TARGET THEN
        {expr}'''

MERGE_WHEN_NOT_MATCHED_BY_SOURCE = '''
    WHEN NOT MATCHED BY SOURCE THEN
        {expr}'''

MERGE_INSERT_STATEMENT = '''INSERT ({columns})
        VALUES ({columns_pfx})'''

MERGE_UPDATE_STATEMENT = 'UPDATE SET {update_expr}'
MERGE_DELETE_STATEMENT = 'DELETE'

CHECKSUM_AGG_STATEMENT = '''
    SELECT CHECKSUM_AGG(CHECKSUM({columns})) value FROM {table}
'''

VALUES_CONSTRUCTOR = 'VALUES {values}'
