from uuid import UUID
from datetime import date, datetime
import itertools
import re

import pytest

from csvbase.web.main.bp import from_html_form_to_python, make_table_view_etag
from csvbase.value_objs import (
    ColumnType,
    Column,
    ContentType,
    Table,
    DataLicence,
    RowCount,
    KeySet,
)


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


def test_table_view_etag():
    """Test that the etags for table views come out short enough"""

    table_tuples = [
        (UUID("f" * 32), datetime(2018, 1, 3, 9)),
        (UUID("f" * 32), datetime(2018, 1, 3, 9, 1)),
    ]
    a = Column("a", ColumnType.INTEGER)
    tables = [
        Table(
            t[0],
            username="someone",
            table_name="a-table",
            is_public=False,
            caption="",
            data_licence=DataLicence.ALL_RIGHTS_RESERVED,
            columns=[a],
            created=datetime(2018, 1, 3, 9),
            row_count=RowCount(0, 0),
            last_changed=t[1],
        )
        for t in table_tuples
    ]

    keysets = [
        KeySet([a], values=(10,), op="greater_than"),
        KeySet([a], values=(11,), op="greater_than"),
        KeySet([a], values=(10,), op="less_than"),
    ]

    arg_list = list(itertools.product(tables, ContentType, keysets))
    etags = [make_table_view_etag(*args) for args in arg_list]
    assert len(set(etags)) == len(arg_list), "duplicate etags found"

    assert [e for e in etags if len(e) > 256] == [], "etag too long"

    etag_regex = re.compile(r'W/"[A-Za-z0-9\-\._]+\.[A-Za-z0-9\-\._]+"')
    for e in etags:
        assert etag_regex.match(e), e
