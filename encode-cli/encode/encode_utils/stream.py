import asyncio
import aiohttp
import json
import operator
import os
import numpy as np
import pandas as pd
import requests

from functools import wraps
from itertools import chain
from urllib.parse import urljoin

operator_map = {'equals': operator.eq,
                'not_equals': operator.ne,
                'contains': operator.contains}


def process_stream(processors):
    """
    Apply all processors to stream. Requires yield_files(files) to be passed
    in as first item in list.
    """
    stream = ()
    for processor in processors:
        stream = processor(stream)
    return stream


def processor(f):
    """
    Return processor function applied to stream.
    """
    @wraps(f)
    def new_func(*args, **kwargs):
        def processor(stream):
            return f(stream, *args, **kwargs)
        return processor
    return new_func


def generator(f):
    """
    Return function that provides original data to stream.
    """
    @wraps(f)
    @processor
    def new_func(stream, *args, **kwargs):
        yield from f(*args, **kwargs)
    return new_func


@generator
def yield_files(files):
    """
    Initiate processing stream with files.
    """
    for file in files:
        yield file


@processor
def filter_field_by_comparison(stream, field=None, value=None, comparison='equals'):
    """
    Filter list of dictionaries based on field value, filter value, and comparison.

    Parameters
    ----------
    stream : generator
        Original data plus applied processing steps passed in by @processor decorator.
    field : string
        Name of field in dictionary.
    value : string or list (for contains)
        Filter value to compare to field value.
    comparison : {'equals' | 'not_equals' | 'contains'}
        Operator used for comparing values. Default is equals.
    """
    if operator_map.get(comparison) is None:
        raise ValueError('Comparison must be one of: {}'.format(
            list(operator_map.keys())))
    for file in stream:
        # Make sure filter value exists.
        if value is None:
            raise ValueError('Must specify value')
        # Continue if key not in specific file.
        if file.get(field) is None:
            continue
        # Order of variables matter for contains.
        left, right = value, file.get(field)
        if comparison == 'contains' and isinstance(value, str):
            left, right = file.get(field), value
        # Yield only files that match filter.
        if operator_map[comparison](left, right):
            yield file


def match(data, *args):
    """
    Pass in data and filters.
    """
    yield from process_stream([s for s in chain([yield_files(data)], [*args])])


def _extract_values_from_pattern(field, in_type, out_type):
    """
    Returns set of values in both in_type and out_type for given field.
    """
    values = []
    for value in chain([in_type], [out_type]):
        # Flatten if value is a list.
        if isinstance(value.get(field), list):
            values.extend(value.get(field))
        else:
            values.append(value.get(field))
    if None in values:
        raise ValueError('Must specify {} in pattern.'.format(field))
    return set(values)
