import itertools

from csvbase.value_objs import ColumnType

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
    assert ColumnType.BOOLEAN.from_string_to_python(bool_str) == expected


@pytest.mark.parametrize(
    "form_value, expected",
    [
        ("on", True),
        (None, False),
    ],
)
def test_bool_parsing_from_html_form(form_value, expected):
    assert ColumnType.BOOLEAN.from_html_form_to_python(form_value) == expected
