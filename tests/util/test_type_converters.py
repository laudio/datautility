''' Tests for type converter utilites. '''

import pytest
import datetime

from datautility.util.type_converters import (
    integer,
    boolean,
    timestamp,
    time,
    date,
    string,
    floating
)


def test_integer():
    ''' Test integer converter.'''

    assert (integer()('123') == 123)
    assert isinstance(integer()('123'), int)

    assert (integer()('') == None)

    # Throws value error when non integer value is passed

    with pytest.raises(ValueError) as ex:
        integer()('123foo')

    with pytest.raises(ValueError) as ex:
        integer()('123.123')

    with pytest.raises(ValueError) as ex:
        integer(False)('')


def test_floating():
    ''' Test floating converter.'''
    assert floating()('123.123') == 123.123
    assert isinstance(floating()('123.123'), float)

    assert floating()('123') == 123.0
    assert isinstance(floating()('123.123'), float)

    assert floating()(123) == 123.0
    assert isinstance(floating()('123.123'), float)

    assert floating()('') == None

    with pytest.raises(ValueError) as ex:
        floating()('123.123.123')

    with pytest.raises(ValueError) as ex:
        floating()('hello')

    with pytest.raises(ValueError) as ex:
        floating(False)('')


def test_boolean():
    ''' Test boolean converter.'''

    assert boolean()('0') == False
    assert isinstance(boolean()('0'), bool)

    assert boolean()('False') == False
    assert isinstance(boolean()('0'), bool)

    assert boolean()('True') == True
    assert isinstance(boolean()('True'), bool)

    assert boolean()('123') == True
    assert isinstance(boolean()('True'), bool)

    assert boolean()('123.123') == True
    assert isinstance(boolean()('True'), bool)

    assert boolean()('abc') == True
    assert isinstance(boolean()('True'), bool)

    assert boolean()('') == None

    assert boolean(False)('') == False


def test_timestamp():
    ''' Test timestamp converter.'''

    test = timestamp()('2019-04-26 16:55:57.8233333')
    assert isinstance(test, datetime.datetime)
    assert test.year == 2019
    assert test.month == 4
    assert test.day == 26
    assert test.hour == 16
    assert test.minute == 55
    assert test.second == 57
    assert test.microsecond == 823330

    test2 = timestamp()('2019-04-26 16:55:57')
    assert isinstance(test2, datetime.datetime)
    assert test2.year == 2019
    assert test2.month == 4
    assert test2.day == 26
    assert test2.hour == 16
    assert test2.hour == 16
    assert test2.minute == 55
    assert test2.second == 57

    assert timestamp()('') == None

    # Throws value error when value isn't is correct format

    with pytest.raises(ValueError) as ex:
        timestamp()('26-04-2019 16:55:57.82333')

    with pytest.raises(ValueError) as ex:
        timestamp()('2019-4-26 16:55:57')

    with pytest.raises(ValueError) as ex:
        timestamp()('2019-04-26')

    with pytest.raises(ValueError) as ex:
        timestamp()('foo bar')

    with pytest.raises(ValueError) as ex:
        timestamp(False)('')


def test_date():
    ''' Test date converter.'''
    test = date()('2019-04-26')
    assert isinstance(test, datetime.date)
    assert test.year == 2019
    assert test.month == 4
    assert test.day == 26

    assert date()('') == None

    # Throws value error when value isn't is correct format

    with pytest.raises(ValueError) as ex:
        date()('26-04-2019')

    with pytest.raises(ValueError) as ex:
        date()('foo bar')

    with pytest.raises(ValueError) as ex:
        date(False)('')


def test_time():
    ''' Test time converter.'''
    test = time()('23:59:58')
    assert isinstance(test, datetime.time)
    assert test.hour == 23
    assert test.minute == 59
    assert test.second == 58

    assert time()('') == None

    # Throws value error when invalid time is given

    with pytest.raises(ValueError) as ex:
        time()('24:59:58')

    with pytest.raises(ValueError) as ex:
        time()('23:60:58')

    with pytest.raises(ValueError) as ex:
        time()('23:59:60')

    with pytest.raises(ValueError) as ex:
        time()('23 :59:58')

    with pytest.raises(ValueError) as ex:
        time(False)('')


def test_string():
    ''' Test string converter.'''
    assert string()(123) == '123'
    assert isinstance(string()(123), str)

    assert string()('') == None

    assert string(False)('') == ''
