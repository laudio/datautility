''' Tests for data loader utilities. '''

import pytest
import datetime
from mock import patch, mock_open, MagicMock

from datautility.util.data.exceptions import ValidationError
from datautility.util.data.loader import (
    convert_data_types,
    validate,
    sync_from_csv,
    validate_columns
)
from datautility.util.type_converters import (
    integer,
    boolean,
    timestamp,
    date,
    string,
    time,
    floating
)

SAMPLE_CSV_1 = '''\
id,name,description
1,hello,'world'
2,foo's,bar
3,python,"rocks"
'''

SAMPLE_CSV_2 = '''\
id,name,description
1,,'world'
2,foo's,
3,python,"rocks"
'''

SAMPLE_CSV_3 = '''\
id,name,dob,created_at
1,foo,2019-04-26,2019-04-26 16:55:57.8233333
2,bar,2019-04-26,2019-04-26 16:55:57.8233333
3,hello,2019-04-26,
4,world,,2019-04-26 16:55:57.8233333
'''

SAMPLE_CSV_4 = '''\
name,is_active,average_checkin_time
foo,false,16:55:57
bar,0,16:55:57
hello,1,
world,,16:55:57
'''


def test_convert_data_types_1():
    ''' Test convert_data_type works with header data. '''

    data = [
        ['id', 'name', 'description'],
        ['1', 'Foo', 'Bar'],
        ['2', 'Hello', 'World']
    ]

    output = [
        ['id', 'name', 'description'],
        [1, 'Foo', 'Bar'],
        [2, 'Hello', 'World']
    ]

    types = [integer(), string(), string()]

    assert convert_data_types(data, types) == output


def test_convert_data_types_2():
    ''' Test convert_data_type works without header data. '''

    data = [
        ['1', '50.55', 'Foo', 'Bar'],
        ['2', '154.5', 'Hello', 'World']
    ]

    output = [
        [1, 50.55, 'Foo', 'Bar'],
        [2, 154.5, 'Hello', 'World']
    ]

    has_header = False

    types = [integer(), floating(), string(), string()]

    assert convert_data_types(data, types, has_header) == output


def test_convert_data_types_3():
    ''' Test convert_data_type throws exception with consistent data and invalid number of type definitions. '''

    data = [
        ['1', '50.55', 'Foo', 'Bar'],
        ['2', '154.5', 'Hello', 'World']
    ]

    has_header = False

    types = [floating(), string(), string()]

    with pytest.raises(AssertionError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == 'Mismatch in number of column type definitions and number of columns in data.'


def test_convert_data_types_4():
    ''' Test convert_data_type throws exception with inconsistent data.'''

    data = [
        ['1', '50.55', 'Foo', 'Bar'],
        ['2', '154.5', 'Hello'],
        ['3', '75.55', 'Python', 'Rocks']
    ]

    has_header = False

    types = [integer(), floating(), string(), string()]

    with pytest.raises(AssertionError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0][0:26] == 'Number of columns mismatch'


def test_convert_data_types_5():
    ''' Test convert_data_type throws exception when string is passed to integer type_converter.'''

    data = [
        ['1', 'Foo', 'Bar'],
        ['2', 'Hello', 'World'],
        ['Garbage', 'Python', 'Rocks']
    ]

    has_header = False

    types = [integer(), string(), string()]

    with pytest.raises(ValueError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == "invalid literal for int() with base 10: 'Garbage'"


def test_convert_data_types_6():
    ''' Test convert_data_type throws exception when string is passed to floating type_converter.'''

    data = [
        ['1', '50.55', 'Foo', 'Bar'],
        ['2', 'Garbage', 'Hello', 'World'],
        ['3', '75.55', 'Python', 'Rocks']
    ]

    has_header = False

    types = [integer(), floating(), string(), string()]

    with pytest.raises(ValueError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == "could not convert string to float: 'Garbage'"


def test_convert_data_types_7():
    ''' Test convert_data_type throws exception when date with invalid format is given.'''

    data = [
        ['1', '2019-04-26'],
        ['2', '2019-04-26'],
        ['3', '26-04-2019']
    ]

    has_header = False

    types = [integer(), date()]

    with pytest.raises(ValueError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == "invalid literal for int() with base 10: '26-0'"


def test_convert_data_types_8():
    ''' Test convert_data_type throws exception when invalid date is given.'''

    data = [
        ['1', '2019-04-26'],
        ['2', '2019-04-26'],
        ['3', '2019-04-70']
    ]

    has_header = False

    types = [integer(), date()]

    with pytest.raises(ValueError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == "day is out of range for month"


def test_convert_data_types_9():
    ''' Test convert_data_type throws exception when time with invalid format is given.'''

    data = [
        ['1', '23 :59:58'],
        ['2', '23:59:58'],
        ['3', '23:59:58']
    ]

    has_header = False

    types = [integer(), time()]

    with pytest.raises(ValueError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == "invalid literal for int() with base 10: ':5'"


def test_convert_data_types_10():
    ''' Test convert_data_type throws exception when invalid time is given.'''

    data = [
        ['1', '23:59:58'],
        ['2', '23:59:58'],
        ['3', '23:59:60']
    ]

    has_header = False

    types = [integer(), time()]

    with pytest.raises(ValueError) as ex:
        convert_data_types(data, types, has_header)

    assert ex.value.args[0] == 'second must be in 0..59'


def test_convert_data_types_11():
    ''' Test convert_data_type works when type converters are given in dictionary. '''

    data = [
        ['id', 'name', 'description'],
        ['1', 'Foo', 'Bar'],
        ['2', 'Hello', 'World']
    ]

    output = [
        ['id', 'name', 'description'],
        [1, 'Foo', 'Bar'],
        [2, 'Hello', 'World']
    ]

    types = {
        'name': string(),
        'id': integer(),
        'description': string()
    }

    assert convert_data_types(data, types) == output


def test_convert_data_types_12():
    ''' Test convert_data_type throws error corresponding column isn't provided type converters. '''

    data = [
        ['id', 'name', 'description'],
        ['1', 'Foo', 'Bar'],
        ['2', 'Hello', 'World']
    ]

    output = [
        ['id', 'name', 'description'],
        [1, 'Foo', 'Bar'],
        [2, 'Hello', 'World']
    ]

    types = {
        'name': string(),
        'garbage': integer(),
        'description': string()
    }

    with pytest.raises(KeyError) as ex:
        convert_data_types(data, types)

    assert ex.value.args[0] == "Unable to locate 'id' column in type converters dictionary"


def test_convert_data_types_13():
    ''' Test convert_data_type throws error when number of type converters in dictionary in less. '''

    data = [
        ['id', 'name', 'description'],
        ['1', 'Foo', 'Bar'],
        ['2', 'Hello', 'World']
    ]

    output = [
        ['id', 'name', 'description'],
        [1, 'Foo', 'Bar'],
        [2, 'Hello', 'World']
    ]

    types = {
        'name': string(),
        'description': string()
    }

    with pytest.raises(AssertionError) as ex:
        convert_data_types(data, types)

    assert ex.value.args[0] == "Mismatch in number of column type definitions and number of columns in data."


def test_validate_1():
    ''' Test validate. '''

    consistent_data = [
        ['1', '50.55', 'Foo'],
        ['2', '154.5', 'Hello']
    ]

    validate(consistent_data)

    inconsistent_data = [
        ['1', '50.55', 'Foo'],
        ['2', '154.5', 'Hello', 'World']
    ]

    with pytest.raises(AssertionError) as ex:
        validate(inconsistent_data)

    assert ex.value.args[0][0:26] == 'Number of columns mismatch'


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_1(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv works without explict column type definitions. '''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    expected_data = [
        ['id', 'name', 'description'],
        ['1', 'hello', "'world'"],
        ['2', "foo's", 'bar'],
        ['3', 'python', 'rocks']
    ]

    params = {
        'table': 'sometable'
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_1)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)

        # Would have thrown assertion exception if gen_sync_sql had received unexpected data.
        gen_sync_sql_mock.assert_called_once_with(expected_data, **params)


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_2(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv works with explict column type definitions.'''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    expected_data = [
        ['id', 'name', 'description'],
        [1, 'hello', "'world'"],
        [2, "foo's", 'bar'],
        [3, 'python', 'rocks']
    ]

    # True is the default parameter in all type specifier
    params = {
        'table': 'sometable',
        'types': [integer(), string(), string()]
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_1)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)

        gen_sync_sql_mock.assert_called_once_with(expected_data, **params)


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_3(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv when the data consists of empty values.'''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    expected_data = [
        ['id', 'name', 'description'],
        [1, '', "'world'"],
        [2, "foo's", ''],
        [3, 'python', 'rocks']
    ]

    params = {
        'table': 'sometable',
        'types': [integer(), string(False), string(False)]
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_2)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)

        gen_sync_sql_mock.assert_called_once_with(expected_data, **params)


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_4(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv when the data consists of empty values and defining them as None.'''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    expected_data = [
        ['id', 'name', 'description'],
        [1, None, "'world'"],
        [2, "foo's", None],
        [3, 'python', 'rocks']
    ]

    params = {
        'table': 'sometable',
        'types': [integer(), string(), string()]
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_2)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)

        gen_sync_sql_mock.assert_called_once_with(expected_data, **params)


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_5(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv with date and timestamp.'''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    expected_data = [
        ['id', 'name', 'dob', 'created_at'],
        [1, 'foo', datetime.date(2019, 4, 26), datetime.datetime(2019, 4, 26, 16, 55, 57, 823330)],
        [2, 'bar', datetime.date(2019, 4, 26), datetime.datetime(2019, 4, 26, 16, 55, 57, 823330)],
        [3, 'hello', datetime.date(2019, 4, 26), None],
        [4, 'world', None, datetime.datetime(2019, 4, 26, 16, 55, 57, 823330)]
    ]

    params = {
        'table': 'sometable',
        'types': [integer(False), string(), date(), timestamp()]
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_3)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)
        gen_sync_sql_mock.assert_called_once_with(expected_data, **params)


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_6(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv with boolean and time.'''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    expected_data = [
        ['name', 'is_active', 'average_checkin_time'],
        ['foo', False, datetime.time(16, 55, 57)],
        ['bar', False, datetime.time(16, 55, 57)],
        ['hello', True, None],
        ['world', None, datetime.time(16, 55, 57)]
    ]

    params = {
        'table': 'sometable',
        'types': {
            'name': string(),
            'is_active': boolean(),
            'average_checkin_time': time()
        }
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_4)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)
        gen_sync_sql_mock.assert_called_once_with(expected_data, **params)


@patch('datautility.util.data.loader.db.connect')
@patch('datautility.util.data.loader.gen_sync_sql')
def test_sync_from_csv_7(gen_sync_sql_mock, db_connect_mock):
    ''' Test sync_from_csv throws exception when receives unexpected data.'''

    filename = 'somefile'

    target_db = {
        'driver': 'hello',
        'host': 'world',
        'schema': 'foo',
        'login': 'bar',
        'password': 'python',
        'port': 1234
    }

    params = {
        'table': 'sometable',
    }

    with patch('builtins.open', mock_open(read_data=SAMPLE_CSV_1)) as mock_file:
        sync_from_csv(target_db, mock_file, **params)

        with pytest.raises(AssertionError) as ex:
            gen_sync_sql_mock.assert_called_once_with('garbage data', **params)


def test_validate_columns1():
    ''' Test: Should pass for both argument having same columns. '''
    source_columns = [
        ('id', 'int', 4),
        ('name', 'varchar', 100),
        ('description', 'varchar', 2000)
    ]
    # It should execute without failure.
    validate_columns(source_columns, source_columns)


def test_validate_columns2():
    ''' Test: Should throw exception if columns are not present in target table.  '''
    source_columns = [
        ('id', 'int', 4),
        ('name', 'varchar', 100),
        ('description', 'varchar', 2000)
    ]
    target_columns = [
        ('id', 'int', 4),
        ('title', 'varchar', 100)
    ]

    with pytest.raises(ValidationError):
        validate_columns(source_columns, target_columns)


def test_validate_columns3():
    ''' Test: Should throw exception if columns datatype is mismatched. '''
    source_columns = [
        ('id', 'int', 4), ('title', 'nvarchar', 100)
    ]
    target_columns = [
        ('id', 'int', 4), ('title', 'varchar', 1000)
    ]

    with pytest.raises(ValidationError):
        validate_columns(source_columns, target_columns)


def test_validate_columns4():
    ''' Test: Should pass even if the target columns doesn't have the all source columns. '''
    source_columns = [
        ('id', 'int', 4), ('title', 'varchar', 100)
    ]
    target_columns = [
        ('id', 'int', 4),
        ('title', 'varchar', 100),
        ('description', 'nvarchar', 1000)
    ]

    # It should execute without failure.
    validate_columns(source_columns, target_columns)
