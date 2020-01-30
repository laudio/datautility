''' Tests for sqlgen. '''

import pytest
from datautility.util.data import sqlgen


def test_gen_values_sql_1():
    ''' Case 1 - simple. '''
    rows = [
        [1, 'Hello World'],
        [2, 'Foo Bar']
    ]

    sql = sqlgen.gen_values_sql(rows)

    assert sql == '''VALUES (1, N'Hello World'), (2, N'Foo Bar')'''


def test_gen_values_sql_2():
    ''' Case 2 - string with quotes. '''
    rows = [
        [1, 'String with \'single\' quotes.'],
        [2, 'String with "double" quotes.']
    ]

    sql = sqlgen.gen_values_sql(rows)

    assert sql == '''VALUES (1, N'String with ''single'' quotes.'), (2, N'String with "double" quotes.')'''


def test_gen_values_sql_3():
    ''' Case 3 - boolean, None and other values. '''
    rows = [
        [True, False, None, 5, 5.679333],
        [False, None, True, 0, 1.00001],
        [True, True, True, -1, -0.000009]
    ]

    sql = sqlgen.gen_values_sql(rows)

    assert sql == '''VALUES (1, 0, NULL, 5, 5.679333), (0, NULL, 1, 0, 1.00001), (1, 1, 1, -1, -9e-06)'''


def test_gen_values_sql_4():
    '''Case 4 - escaped strings and possible SQL injections. '''
    rows = [
        ['''Test's'''],
        ['''Test\''''],
        ['''Test\\"'''],
        # TODO: Todo identity and add more cases for SQL-injection.
    ]

    sql = sqlgen.gen_values_sql(rows)
    assert sql == '''VALUES (N'Test''s'), (N'Test\'\''), (N'Test\\"')'''


def test_gen_values_sql_5():
    '''Case 5 - If list is empty. '''
    rows = []

    sql = sqlgen.gen_values_sql(rows)
    assert sql == None


def test_gen_sync_sql_1():
    ''' Basic case - returns sql for the simple case. '''
    data = [
        ['id', 'name'],
        [1, 'Foo'],
        [2, 'Bar'],
        [3, '"double" and \'single\' quotes.']
    ]

    sql = sqlgen.gen_sync_sql(data, table='test_table')

    assert sql == '''
    MERGE test_table t USING (VALUES (1, N'Foo'), (2, N'Bar'), (3, N'"double" and ''single'' quotes.')) AS s(id, name) ON t.id = s.id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, name)
        VALUES (s.id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_sync_sql_2():
    ''' A case with identity insert enabled. '''
    data = [
        ['id', 'name'],
        [1, 'Foo'],
        [2, 'Bar'],
        [3, '"double" and \'single\' quotes.']
    ]

    sql = sqlgen.gen_sync_sql(data, table='test_table', identity_insert=True)

    assert sql == '''\
    SET IDENTITY_INSERT test_table ON;
    MERGE test_table t USING (VALUES (1, N'Foo'), (2, N'Bar'), (3, N'"double" and ''single'' quotes.')) AS s(id, name) ON t.id = s.id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, name)
        VALUES (s.id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
    SET IDENTITY_INSERT test_table OFF;\
'''


def test_gen_sync_sql_3():
    ''' A case with multiple comparators - should have AND condition. '''
    data = [
        ['user_id', 'role_id', 'name'],
        [1, 2, 'Foo'],
        [2, 4, 'Bar'],
        [3, 6, 'Tar']
    ]

    sql = sqlgen.gen_sync_sql(
        data,
        table='test_table',
        comparators=['user_id', 'role_id']
    )

    assert sql == '''
    MERGE test_table t USING (VALUES (1, 2, N'Foo'), (2, 4, N'Bar'), (3, 6, N'Tar')) AS s(user_id, role_id, name) ON t.user_id = s.user_id AND t.role_id = s.role_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_sync_sql_4():
    ''' A case with contition=True - should have condition with MATCHED. '''
    data = [
        ['user_id', 'role_id', 'name'],
        [1, 2, 'Foo'],
        [2, 4, 'Bar'],
        [3, 6, 'Tar']
    ]

    sql = sqlgen.gen_sync_sql(
        data,
        table='test_table',
        comparators=['user_id', 'role_id'],
        diff=True
    )

    assert sql == '''
    MERGE test_table t USING (VALUES (1, 2, N'Foo'), (2, 4, N'Bar'), (3, 6, N'Tar')) AS s(user_id, role_id, name) ON t.user_id = s.user_id AND t.role_id = s.role_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED AND CHECKSUM(t.name) <> CHECKSUM(s.name) THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_1():
    ''' Basic case - returns sql for the simple case. '''
    sql = sqlgen.gen_merge_sql(
        keys=['id', 'name'],
        source_table='test_table1',
        target_table='test_table2'
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.id = s.id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, name)
        VALUES (s.id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_2():
    ''' A case with identity insert enabled. '''
    sql = sqlgen.gen_merge_sql(
        keys=['id', 'name', 'description'],
        source_table='test_table1',
        target_table='test_table2',
        identity_insert=True
    )

    assert sql == '''\
    SET IDENTITY_INSERT test_table2 ON;
    MERGE test_table2 t USING test_table1 s ON t.id = s.id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, name, description)
        VALUES (s.id, s.name, s.description)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name, t.description = s.description
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
    SET IDENTITY_INSERT test_table2 OFF;\
'''


def test_gen_merge_sql_3():
    ''' A case with multiple comparators - should have AND condition. '''
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        keys=['user_id', 'role_id', 'name'],
        comparators=['user_id', 'role_id']
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.user_id = s.user_id AND t.role_id = s.role_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_4():
    ''' A case with diff=True - should have condition with MATCHED. '''
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        keys=['user_id', 'role_id', 'name'],
        comparators=['user_id', 'role_id'],
        diff=True
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.user_id = s.user_id AND t.role_id = s.role_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED AND CHECKSUM(t.name) <> CHECKSUM(s.name) THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_5():
    ''' Should raise the exception if keys is not passed. '''
    with pytest.raises(KeyError, match='`keys` must be provided in params.'):
        sqlgen.gen_merge_sql(
            source_table='test_table1',
            target_table='test_table2',
            comparators=['user_id', 'role_id']
        )


def test_gen_merge_sql_6():
    ''' A case with custom_params with when_not_matched_by_source=False - shouldn't have delete statement. '''
    custom_params = {
        'when_not_matched_by_source': False
    }
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        diff=True,
        keys=['user_id', 'role_id', 'name'],
        comparators=['user_id', 'role_id'],
        custom_params=custom_params
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.user_id = s.user_id AND t.role_id = s.role_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED AND CHECKSUM(t.name) <> CHECKSUM(s.name) THEN
        UPDATE SET t.name = s.name;
'''


def test_gen_merge_sql_7():
    ''' A case with all custom params in merge statement. '''
    custom_params = {
        'when_not_matched_by_target': {
            'statement_type': 'update',
            'params': {'t.name': 's.name'}
        },
        'when_matched': {
            'statement_type': 'insert',
            'params': {'user_id': 's.user_id', 'role_id': 's.role_id', 'name': 's.name'}
        },
        'when_not_matched_by_source': {
            'statement_type': 'delete'
        }
    }
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        keys=['user_id', 'role_id', 'name'],
        comparators=['user_id', 'role_id'],
        custom_params=custom_params
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.user_id = s.user_id AND t.role_id = s.role_id
    WHEN NOT MATCHED BY TARGET THEN
        UPDATE SET t.name = s.name
    WHEN MATCHED THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_8():
    ''' A case with exclude keys. '''
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        exclude=['created_at', 'updated_at'],
        keys=['id', 'role_id', 'name', 'created_at', 'updated_at'],
        diff=True
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.id = s.id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, role_id, name)
        VALUES (s.id, s.role_id, s.name)
    WHEN MATCHED AND CHECKSUM(t.role_id, t.name) <> CHECKSUM(s.role_id, s.name) THEN
        UPDATE SET t.role_id = s.role_id, t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_9():
    ''' A case with all custom params including custom comparators in merge statement. '''
    custom_params = {
        'when_not_matched_by_target': {
            'statement_type': 'update',
            'params': {'t.name': 's.fullname'}
        },
        'when_matched': {
            'statement_type': 'insert',
            'params': {'user_id': 's.employee_id', 'role_id': 's.user_role_id', 'name': 's.fullname'}
        },
        'when_not_matched_by_source': {
            'statement_type': 'delete'
        }
    }
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        comparators={'t.user_id': 's.employee_id', 't.role_id': 's.user_role_id'},
        custom_params=custom_params
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.user_id = s.employee_id AND t.role_id = s.user_role_id
    WHEN NOT MATCHED BY TARGET THEN
        UPDATE SET t.name = s.fullname
    WHEN MATCHED THEN
        INSERT (user_id, role_id, name)
        VALUES (s.employee_id, s.user_role_id, s.fullname)
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_merge_sql_10():
    ''' A case with all custom conditional statement in merge statement. '''
    custom_params = {
        'when_not_matched_by_target': {
            'statement_type': 'update',
            'params': {'t.name': 's.fullname'}
        },
        'when_matched': {
            'statement_type': 'insert',
            'params': {'user_id': 's.employee_id', 'role_id': 's.user_role_id', 'name': 's.fullname'}
        },
        'when_not_matched_by_source': {
            'statement_type': 'delete'
        }
    }
    sql = sqlgen.gen_merge_sql(
        source_table='test_table1',
        target_table='test_table2',
        comparators={'t.user_id': 's.employee_id', 'ISNULL(t.role_id, 0)': 'ISNULL(s.user_role_id, 0)'},
        custom_params=custom_params
    )

    assert sql == '''
    MERGE test_table2 t USING test_table1 s ON t.user_id = s.employee_id AND ISNULL(t.role_id, 0) = ISNULL(s.user_role_id, 0)
    WHEN NOT MATCHED BY TARGET THEN
        UPDATE SET t.name = s.fullname
    WHEN MATCHED THEN
        INSERT (user_id, role_id, name)
        VALUES (s.employee_id, s.user_role_id, s.fullname)
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
'''


def test_gen_comparator_query_1():
    ''' Basic case. Should return expected query. '''
    sql = sqlgen.gen_comparator_query(keys=['s.name', 's.description'])

    assert sql == 'CHECKSUM(s.name, s.description)'


def test_gen_merge_insert_statement_1():
    ''' Basic case. Should return expected query. '''
    keys = {
        "name": "s.name",
        "description": "s.description",
    }
    sql = sqlgen.gen_merge_insert_statement(keys)

    assert sql == '''INSERT (name, description)
        VALUES (s.name, s.description)'''


def test_gen_merge_insert_statement_2():
    ''' Should return expected query if target columns are different. '''
    keys = {
        'title': 's.name',
        'remarks': 's.description',
    }
    sql = sqlgen.gen_merge_insert_statement(keys)

    assert sql == '''INSERT (title, remarks)
        VALUES (s.name, s.description)'''


def test_gen_merge_update_statement_1():
    ''' Basic case. Should return expected query. '''
    keys = {
        't.name': 's.name',
        't.description': 's.description',
    }
    sql = sqlgen.gen_merge_update_statement(keys)

    assert sql == 'UPDATE SET t.name = s.name, t.description = s.description'


def test_gen_merge_update_statement_2():
    ''' Should return expected query if target columns are different. '''
    keys = {
        't.title': 's.name',
        't.remarks': 's.description',
    }
    sql = sqlgen.gen_merge_update_statement(keys)

    assert sql == 'UPDATE SET t.title = s.name, t.remarks = s.description'


def test_get_stmt_by_condition_1():
    ''' Basic case. Should return expected query for `when matched` case. '''
    sql = sqlgen.get_stmt_by_condition(
        condition_name='when_matched',
        statement_type='update',
        checksum_params={'t.id': 's.id', 't.name': 's.name'},
        params={'is_active': '0'},
        diff=True
    )

    assert sql == '''
    WHEN MATCHED AND CHECKSUM(t.id, t.name) <> CHECKSUM(s.id, s.name) THEN
        UPDATE SET is_active = 0'''


def test_get_stmt_by_condition_2():
    ''' Basic case. Should return expected query for `when_not_matched_by_target` case. '''
    sql = sqlgen.get_stmt_by_condition(
        condition_name='when_not_matched_by_target',
        statement_type='delete'
    )

    assert sql == '''
    WHEN NOT MATCHED BY TARGET THEN
        DELETE'''


def test_get_stmt_by_condition_3():
    ''' Basic case. Should return expected query for `when_not_matched_by_source` case. '''
    sql = sqlgen.get_stmt_by_condition(
        statement_type='insert',
        condition_name='when_not_matched_by_source',
        params={'id': 's.id', 'name': 's.name'}
    )
    assert sql == '''
    WHEN NOT MATCHED BY SOURCE THEN
        INSERT (id, name)
        VALUES (s.id, s.name)'''


def test_zip_insert_params_1():
    ''' Basic case. Should return insert dictionary of given keys. '''
    keys = ['id', 'name']
    res = sqlgen.zip_insert_params(keys)

    assert res == {'id': 's.id', 'name': 's.name'}


def test_zip_params_1():
    ''' Basic case. Should return dictionary of given keys. '''
    keys = ['id', 'name']
    res = sqlgen.zip_params(keys)

    assert res == {'t.id': 's.id', 't.name': 's.name'}


def test_merge_and_gen_stmt_1():
    ''' A case to generate custom query without custom params. '''
    sql = sqlgen.merge_and_gen_stmt(
        source_table='test_table1',
        target_table='test_table2',
        update_keys=['name', 'name'],
        insert_keys=['user_id', 'role_id', 'name']
    )

    assert sql == '''
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE'''


def test_merge_and_gen_stmt_2():
    ''' A case to generate custom query with `when_not_matched_by_source` '''
    custom_params = {
        'when_not_matched_by_source': {
            'statement_type': 'update',
            'params': {
                't.is_active': 0
            }
        }
    }
    sql = sqlgen.merge_and_gen_stmt(
        source_table='test_table1',
        target_table='test_table2',
        update_keys=['name', 'name'],
        insert_keys=['user_id', 'role_id', 'name'],
        custom_params=custom_params
    )

    assert sql == '''
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name
    WHEN NOT MATCHED BY SOURCE THEN
        UPDATE SET t.is_active = 0'''


def test_merge_and_gen_stmt_3():
    ''' A case to generate custom query with all the custom params. '''
    custom_params = {
        'when_not_matched_by_target': {
            'statement_type': 'update',
            'params': {'t.name': 's.name'}
        },
        'when_matched': {
            'statement_type': 'insert',
            'params': {'user_id': 's.user_id', 'role_id': 's.role_id', 'name': 's.name'}
        },
        'when_not_matched_by_source': {
            'statement_type': 'delete'
        }
    }
    sql = sqlgen.merge_and_gen_stmt(
        source_table='test_table1',
        target_table='test_table2',
        update_keys=['name', 'name'],
        insert_keys=['id', 'role_id', 'name'],
        custom_params=custom_params
    )

    assert sql == '''
    WHEN NOT MATCHED BY TARGET THEN
        UPDATE SET t.name = s.name
    WHEN MATCHED THEN
        INSERT (user_id, role_id, name)
        VALUES (s.user_id, s.role_id, s.name)
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE'''


def test_merge_and_gen_stmt_4():
    ''' A case to generate custom query setting `when_matched` to `True` '''
    custom_params = {
        'when_not_matched_by_target': {
            'statement_type': 'insert',
            'params': {'name': 's.name'}
        },
        'when_matched': True,
        'when_not_matched_by_source': False
    }
    sql = sqlgen.merge_and_gen_stmt(
        source_table='test_table1',
        target_table='test_table2',
        update_keys=['name', 'name'],
        insert_keys=['id', 'role_id', 'name'],
        custom_params=custom_params
    )

    assert sql == '''
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (name)
        VALUES (s.name)
    WHEN MATCHED THEN
        UPDATE SET t.name = s.name'''


def test_fetch_all():
    ''' Test fetch_all() returns a SELECT query for a given table. '''
    assert sqlgen.fetch_all('users') == 'SELECT * FROM users'
    assert sqlgen.fetch_all('users', []) == 'SELECT * FROM users'
    assert sqlgen.fetch_all('users', None) == 'SELECT * FROM users'


def test_fetch_all_with_columns():
    ''' Test fetch_all() returns a SELECT query with columns. '''
    result = sqlgen.fetch_all('users', ['id', 'username', 'created_at'])

    assert result == 'SELECT id, username, created_at FROM users'


def test_fetch_all_with_a_single_column():
    ''' Test fetch_all() returns a SELECT query with columns. '''
    result = sqlgen.fetch_all('users', ['id'])

    assert result == 'SELECT id FROM users'


def test_truncate():
    ''' Test truncate() returns a TRUNCATE statement for a given table. '''
    assert sqlgen.truncate('etl.kpis') == 'TRUNCATE TABLE etl.kpis'
