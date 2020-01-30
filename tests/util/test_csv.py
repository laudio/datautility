''' Tests for csv utilites. '''

import pytest
from mock import patch, mock_open

from datautility.util import csv

CSV1_SAMPLE_INPUT = '''\
1,"Eldon's Base for stackable storage shelf, platinum",'Muhammed MacIntyre'
2,"1.7 Cubic Foot Compact ""Cube"" Office Refrigerators",Barry French
3,"Cardinal Slant-D® Ring Binder, Heavy Gauge Vinyl",'Barry French'
4,R380,Clay Rozendal
'''

CSV1_SAMPLE_OUTPUT = [
    ['1', "Eldon's Base for stackable storage shelf, platinum", "'Muhammed MacIntyre'"],
    ['2', '1.7 Cubic Foot Compact "Cube" Office Refrigerators', 'Barry French'],
    ['3', 'Cardinal Slant-D® Ring Binder, Heavy Gauge Vinyl', "'Barry French'"],
    ['4', 'R380', 'Clay Rozendal']
]


def test_csv_load_1():
    ''' Test csv.load() works with simple records. '''

    filename = 'somefile'
    data = 'hello,world\r\nfoo,bar\r\npython,rocks'
    expected_output = [
        ['hello', 'world'],
        ['foo', 'bar'],
        ['python', 'rocks']
    ]

    with patch('builtins.open', mock_open(read_data=data)) as mock_file:
        assert csv.load(filename) == expected_output


def test_csv_load_2():
    ''' Test csv.load() works with data with single and double quotes. '''

    filename = 'somefile'

    with patch('builtins.open', mock_open(read_data=CSV1_SAMPLE_INPUT)) as mock_file:
        assert csv.load(filename) == CSV1_SAMPLE_OUTPUT
