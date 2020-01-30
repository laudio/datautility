''' Tests for data loader utilities. '''

import pytest

import datetime
from mock import Mock, patch, MagicMock

from datautility.util import db


def test_get_columns1():
    ''' Test get_columns() with mock cursor. '''
    cur = Mock()
    cur.columns.return_value = [
        MagicMock(column_name='id', type_name='int', buffer_length=4),
        MagicMock(column_name='name', type_name='varchar', buffer_length=100),
        MagicMock(column_name='description', type_name='varchar', buffer_length=2000)
    ]

    output = db.get_columns(cur, 'test_table1')

    assert output == [
        ('id', 'int', 4),
        ('name', 'varchar', 100),
        ('description', 'varchar', 2000)
    ]


def test_get_columns2():
    ''' Test get_columns() with mock cursor. Add schema. '''
    cur = Mock()
    cur.columns.return_value = [
        MagicMock(column_name='id', type_name='int', buffer_length=4),
        MagicMock(column_name='name', type_name='varchar', buffer_length=100)
    ]

    output = db.get_columns(cur, 'dbo.test_table1')

    assert output == [('id', 'int', 4), ('name', 'varchar', 100)]


def test_get_columns3():
    ''' Test get_columns() with mock cursor. Add database name along with schema. '''
    cur = Mock()
    cur.columns.return_value = [
        MagicMock(column_name='id', type_name='int', buffer_length=4),
        MagicMock(column_name='name', type_name='varchar', buffer_length=100)
    ]

    output = db.get_columns(cur, 'testdb.dbo.test_table1')

    assert output == [('id', 'int', 4), ('name', 'varchar', 100)]
