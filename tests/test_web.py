from datetime import date

import pytest

from csvbase.web.main.bp import from_html_form_to_python
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


@pytest.mark.parametrize(
    "filename",
    ["site.css", "bootstrap.min.css", "bootstrap.bundle.js", "codehilite.css"],
)
def test_static_files(client, filename: str):
    """This test just checks that css has not been broken."""
    response = client.get(f"/static/{filename}")
    assert response.status_code == 200
