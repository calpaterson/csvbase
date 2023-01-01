from datetime import date

import pytest

from csvbase.web import from_html_form_to_python
from csvbase.value_objs import ColumnType


@pytest.mark.parametrize(
    "column_type, form_value, expected",
    [
        (ColumnType.BOOLEAN, "on", True),
        (ColumnType.BOOLEAN, None, False),
        (ColumnType.DATE, "2018-01-03", date(2018, 1, 3)),
        (ColumnType.DATE, "", None),
    ],
)
def test_parsing_from_html_form(column_type, form_value, expected):
    assert from_html_form_to_python(column_type, form_value) == expected
