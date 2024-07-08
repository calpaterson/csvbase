from uuid import UUID
from datetime import date, datetime
import itertools

import pytest

from csvbase.web.main.bp import from_html_form_to_python, make_table_view_etag
from csvbase.web.func import safe_redirect
from csvbase.value_objs import (
    ColumnType,
    Column,
    ContentType,
    Table,
    DataLicence,
    RowCount,
    KeySet,
)
from csvbase import exc
from .utils import assert_is_valid_etag


@pytest.mark.parametrize(
    "column_type, form_value, expected",
    [
        (ColumnType.BOOLEAN, "true", True),
        (ColumnType.BOOLEAN, "na", None),
        (ColumnType.BOOLEAN, "false", False),
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
            key=None,
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

    for e in etags:
        assert_is_valid_etag(e)


@pytest.mark.parametrize(
    "safe_url",
    [
        "/",
        "/user/table",
        "http://localhost/" "http://localhost/user/table",
    ],
)
def test_safe_redirect__happy(app, safe_url):
    with app.app_context():
        response = safe_redirect(safe_url)
    assert response.headers["Location"] == safe_url


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "https://www.google.com/",
        "ftp://localhost.com",
    ],
)
def test_safe_redirect__unhappy(app, unsafe_url):
    with app.app_context():
        with pytest.raises(exc.InvalidRequest):
            safe_redirect(unsafe_url)
