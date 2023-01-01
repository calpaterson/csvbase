import itertools

from csvbase.value_objs import ColumnType
from csvbase.conv import from_string_to_python

import pytest


def _make_versions(inp, expected):
    yield inp.lower(), expected
    yield inp.capitalize(), expected
    yield inp.upper(), expected
    yield inp[0], expected


@pytest.mark.parametrize(
    "bool_str, expected",
    itertools.chain(
        *[
            _make_versions("yes", True),
            _make_versions("no", False),
            _make_versions("true", True),
            _make_versions("false", False),
        ]
    ),
)
def test_bool_parsing_from_string(bool_str, expected):
    assert from_string_to_python(ColumnType.BOOLEAN, bool_str) == expected
