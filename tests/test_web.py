import hashlib
from uuid import UUID
from datetime import date, datetime, timedelta
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
from csvbase import exc, svc
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


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("/", id="index"),
        pytest.param("/newest", id="newest"),
    ],
)
def test_indexes(client, url):
    response = client.get(url)
    assert response.status_code == 200


@pytest.mark.parametrize(
    "url", ["favicon.ico", "apple-touch-icon.png", "apple-touch-icon-precomposed.png"]
)
def test_cache_headers_for_known_absent_urls(client, url):
    """We don't have anything for these urls so ensure that the 404 happens,
    has a relevant error, and is cacheable."""
    response = client.get(url)
    assert response.status_code == 404
    assert response.json == {"error": "that page does not exist"}
    assert response.cache_control.max_age == int(timedelta(days=1).total_seconds())


def test_404_for_unknown_absent_urls(client):
    """Ensure that unknown urls don't generate the error "this user does not
    exist", which is very confusing."""
    response = client.get("/index.php")
    assert response.status_code == 404
    assert response.json == {"error": "that page does not exist"}


def test_security_headers(client):
    resp = client.get("/")
    assert resp.headers["X-Frame-Options"] == "DENY"

    assert resp.headers["X-Content-Type-Options"] == "nosniff"

    assert (
        resp.headers["Content-Security-Policy"]
        == "default-src 'self' https://challenges.cloudflare.com; object-src 'none'; img-src * data:; media-src *;"
    )


def test_avatar__using_a_gravatar(sesh, client, test_user, requests_mocker):
    test_user.settings.use_gravatar = True
    svc.update_user(sesh, test_user)

    email = "example@example.com"
    email_hash = hashlib.sha256(email.encode("utf-8")).hexdigest()
    requests_mocker.get(
        f"https://gravatar.com/avatar/{email_hash}?d=mp", content=b"an image"
    )

    test_user.email = email
    svc.update_user(sesh, test_user)
    sesh.commit()

    resp = client.get(f"/avatars/{test_user.username}")
    assert resp.status_code == 200


def test_avatar__not_using_a_gravatar(sesh, client, test_user, requests_mocker):
    requests_mocker.get(
        f"https://gravatar.com/avatar?d=mp", content=b"a default image"
    )
    resp = client.get(f"/avatars/{test_user.username}")
    assert resp.status_code == 200


def test_avatar__no_email(sesh, client, test_user, requests_mocker):
    requests_mocker.get("https://gravatar.com/avatar?d=mp", content=b"a default image")

    resp = client.get(f"/avatars/{test_user.username}")
    assert resp.status_code == 200
