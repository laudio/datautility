''' String manipulation utility functions. '''

import datetime

from datautility.util.types import is_string


def apply_replace_map(string, mappings):
    '''
    Applies a replace map to provided string.

    param `string` String on which the mappings should be applied.
    type `string` string
    param `mappings` Dictionary defining rules to map provided string to target string.
    type `mappings` dict
    '''
    target_string = string
    for old, new in mappings.items():
        target_string = target_string.replace(old, new)

    return target_string


def format_string(char, iterable):
    '''
    Return a string with given character added before
    the string type element in iterable.
    '''
    formatted = []

    for value in iterable:
        if is_string(value):
            formatted.append("{}'{}'".format(char, escape_quotes(value)))

        elif isinstance(value, datetime.datetime):
            formatted.append("{}'{}'".format(char, escape_quotes(str(value).split('.')[0])))

        else:
            formatted.append(value)

    return "({})".format(', '.join(str(x) for x in formatted))


def escape_quotes(string):
    '''
    Returns a string with single quotes escaped.
    '''
    return string.replace('\'', '\'\'')
