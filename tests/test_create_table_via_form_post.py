from io import BytesIO
from typing import Any, Sequence
from unittest.mock import patch
import pickle

import pandas as pd
from pandas.testing import assert_frame_equal
from werkzeug.datastructures import FileStorage
from flask import url_for
import pytest
from csvbase.web import main
from csvbase.value_objs import ColumnType, Encoding
from csvbase.web.func import set_current_user

from .conftest import ROMAN_NUMERALS
from .utils import random_string, get_df_as_csv, subscribe_user


def render_pickle(*args, **kwargs):
    return pickle.dumps((args, kwargs))


@pytest.fixture(scope="function", autouse=True)
def render_template_to_json():
    with patch.object(main.bp, "render_template") as mock_render_template:
        mock_render_template.side_effect = render_pickle
        yield


TESTCASES: Any = [({}, {"cols": [("", ColumnType.TEXT)]})]


@pytest.mark.parametrize("query, kwargs", TESTCASES)
def test_new_blank_table_form(client, query, kwargs):
    resp = client.get(url_for("csvbase.blank_table"))
    _, template_kwargs = pickle.loads(resp.data)
    template_kwargs.pop("ColumnType")
    template_kwargs.pop("action_url")
    template_kwargs.pop("DataLicence")
    template_kwargs.pop("table_name")
    assert template_kwargs == kwargs


@pytest.mark.parametrize("url", ["/new-table", "/new-table/blank"])
def test_uploading_a_table__invalid_name(client, url, test_user):
    set_current_user(test_user)

    resp = client.post(url, data={"table-name": "some table", "data-licence": 1})

    assert resp.status_code == 400
    assert resp.json == {"error": "that table name is invalid"}


def test_uploading_a_table__not_logged_in(client):
    table_name = f"test-table-{random_string()}"
    username = f"test-{random_string()}"
    resp = client.post(
        "/new-table",
        data={
            "username": username,
            "password": "password",
            "table-name": table_name,
            "data-licence": "1",
            "csv-file": (FileStorage(BytesIO(b"a,b,c\n1,2,3"), "test.csv")),
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{username}/{table_name}"


def test_uploading_a_table__over_quota(client, test_user):
    set_current_user(test_user)

    resp1 = client.post(
        "/new-table",
        data={
            "table-name": "private-table-1",
            "private": "on",
            "data-licence": "1",
            "csv-file": (FileStorage(BytesIO(b"a,b,c\n1,2,3"), "test.csv")),
        },
        content_type="multipart/form-data",
    )
    assert resp1.status_code == 302
    assert resp1.headers["Location"] == f"/{test_user.username}/private-table-1"

    resp2 = client.post(
        "/new-table",
        data={
            "table-name": "private-table-2",
            "private": "on",
            "data-licence": "1",
            "csv-file": (FileStorage(BytesIO(b"a,b,c\n1,2,3"), "test.csv")),
        },
        content_type="multipart/form-data",
    )
    assert resp2.status_code == 400


def test_uploading_a_table(client, test_user):
    table_name = f"test-table-{random_string()}"
    set_current_user(test_user)
    resp = client.post(
        "/new-table",
        data={
            "table-name": table_name,
            "data-licence": "1",
            "csv-file": (FileStorage(BytesIO(b"a,b,c\n1,2,3"), "test.csv")),
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}/{table_name}"


def test_uploading_a_table__csvbase_row_ids(client, test_user, ten_rows):
    """Test that users can export tables with the row ids in them and then
    re-upload them.

    """
    set_current_user(test_user)

    ten_rows_df = get_df_as_csv(
        client, f"/{test_user.username}/{ten_rows.table_name}.csv"
    )

    # non-sequential, and nulls are the next sequence item
    ten_rows_df.index = pd.Series(
        [2, 6, 8, None, 5, 4, 9, 10, 11, 12], dtype="Int64", name="csvbase_row_id"
    )

    csv_buf_for_upload = BytesIO()
    ten_rows_df.to_csv(csv_buf_for_upload)
    csv_buf_for_upload.seek(0)

    new_table_name = f"test-table-{random_string()}"
    resp = client.post(
        "/new-table",
        data={
            "table-name": new_table_name,
            "data-licence": "1",
            "csv-file": (FileStorage(csv_buf_for_upload, "test.csv")),
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302

    expected_df = (
        pd.DataFrame(
            {
                "csvbase_row_id": [2, 6, 8, 13, 5, 4, 9, 10, 11, 12],
                "roman_numeral": ROMAN_NUMERALS,
            }
        )
        .set_index("csvbase_row_id")
        .sort_values(by="csvbase_row_id")
    )
    actual_df = get_df_as_csv(client, resp.headers["Location"])
    assert_frame_equal(expected_df, actual_df[["roman_numeral"]])


@pytest.mark.parametrize(
    "csv_data, encoding, expected_values",
    [
        ("foo\n🪿🪿🪿".encode("utf-8"), Encoding.UTF_8, ["🪿🪿🪿"]),
        ("foo\n£££".encode("latin1"), Encoding.LATIN_1, ["£££"]),
        ("foo\n£££".encode("latin1"), Encoding.ISO8859_2, ["ŁŁŁ"]),
    ],
)
def test_uploading_a_table__encoding(
    client,
    test_user,
    csv_data: bytes,
    encoding: Encoding,
    expected_values: Sequence[str],
):
    """Test that users can specify an encoding when uploading a CSV file and
    that the file data is decoded accordingly.
    """

    table_name = f"test-table-{random_string()}"
    set_current_user(test_user)
    resp = client.post(
        "/new-table",
        data={
            "table-name": table_name,
            "data-licence": "1",
            "encoding": encoding.value,
            "csv-file": (FileStorage(BytesIO(csv_data), "test.csv")),
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}/{table_name}"

    actual_df = get_df_as_csv(client, resp.headers["Location"])
    assert actual_df["foo"].values == expected_values


def test_blank_table(client, test_user):
    set_current_user(test_user)
    resp1 = client.post(
        "/new-table/blank",
        data={
            "col-name-1": "test",
            "col-type-1": "TEXT",
            "table-name": random_string(),
            "data-licence": 1,
            "private": "on",
        },
    )

    assert resp1.status_code == 302


@pytest.mark.xfail(reason="actual bug, needs fixing")
def test_blank_table__and_register(client):
    resp1 = client.post(
        "/new-table/blank",
        data={
            "col-name-1": "test",
            "col-type-1": "TEXT",
            "table-name": random_string(),
            "data-licence": 1,
            "private": "on",
        },
    )

    assert resp1.status_code == 302


def test_blank_table__over_quota(client, test_user):
    set_current_user(test_user)

    resp1 = client.post(
        "/new-table/blank",
        data={
            "col-name-1": "test",
            "col-type-1": "TEXT",
            "table-name": random_string(),
            "data-licence": 1,
            "private": "on",
        },
    )

    assert resp1.status_code == 302

    resp2 = client.post(
        "/new-table/blank",
        data={
            "col-name-1": "test",
            "col-type-1": "TEXT",
            "table-name": random_string(),
            "data-licence": 1,
            "private": "on",
        },
    )

    assert resp2.status_code == 400


def test_second_blank_table__into_subscription_quota(sesh, client, test_user):
    set_current_user(test_user)
    subscribe_user(sesh, test_user)
    sesh.commit()

    resp1 = client.post(
        "/new-table/blank",
        data={
            "col-name-1": "test",
            "col-type-1": "TEXT",
            "table-name": random_string(),
            "data-licence": 1,
            "private": "on",
        },
    )

    assert resp1.status_code == 302

    resp2 = client.post(
        "/new-table/blank",
        data={
            "col-name-1": "test",
            "col-type-1": "TEXT",
            "table-name": random_string(),
            "data-licence": 1,
            "private": "on",
        },
    )

    assert resp2.status_code == 302


def test_copy__form(client, test_user, ten_rows):
    resp = client.get(f"{test_user.username}/{ten_rows.table_name}/copy")
    assert resp.status_code == 200


def test_copy__post(client, test_user, ten_rows):
    set_current_user(test_user)
    new_table_name = random_string()
    new_url = f"/{test_user.username}/{new_table_name}"
    post_resp = client.post(
        f"{test_user.username}/{ten_rows.table_name}/copy",
        data={"table-name": new_table_name},
    )
    assert post_resp.status_code == 302
    assert post_resp.headers["Location"] == new_url

    get_resp = client.get(new_url)
    assert get_resp.status_code == 200


def test_copy__post_private(client, test_user, ten_rows):
    set_current_user(test_user)
    new_table_name = random_string()
    new_url = f"/{test_user.username}/{new_table_name}"
    post_resp = client.post(
        f"{test_user.username}/{ten_rows.table_name}/copy",
        data={"table-name": new_table_name, "private": "on"},
    )
    assert post_resp.status_code == 302
    assert post_resp.headers["Location"] == new_url

    get_resp = client.get(new_url, headers={"Accept": "application/json"})
    assert not get_resp.json["is_public"]
