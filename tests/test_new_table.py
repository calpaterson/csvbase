from io import BytesIO
from typing import Any
from unittest.mock import patch
import pickle

import pandas as pd
from pandas.testing import assert_frame_equal
from werkzeug.datastructures import FileStorage
from flask import url_for
import pytest
from csvbase import web
from csvbase.value_objs import ColumnType

from .conftest import ROMAN_NUMERALS
from .utils import random_string, get_df_as_csv


def render_pickle(*args, **kwargs):
    return pickle.dumps((args, kwargs))


@pytest.fixture(scope="function", autouse=True)
def render_template_to_json():
    with patch.object(web, "render_template") as mock_render_template:
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


def test_uploading_a_table_when_not_logged_in(client):
    table_name = f"test-table-{random_string()}"
    username = f"test_{random_string()}"
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


def test_uploading_a_table(client, test_user):
    table_name = f"test-table-{random_string()}"
    web.set_current_user_for_session(test_user)
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


def test_uploading_a_table_with_csvbase_row_ids(client, test_user, ten_rows):
    """Test that users can export tables with the row ids in them and then
    re-upload them.

    """
    web.set_current_user_for_session(test_user)

    ten_rows_df = get_df_as_csv(client, f"/{test_user.username}/{ ten_rows }.csv")

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
    assert_frame_equal(expected_df, actual_df)
