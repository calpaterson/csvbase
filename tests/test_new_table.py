from io import BytesIO
from typing import Any
from unittest.mock import patch
import pickle

from werkzeug.datastructures import FileStorage
from flask import url_for
import pytest
from csvbase import web
from csvbase.value_objs import ColumnType

from .utils import random_string


def render_pickle(*args, **kwargs):
    return pickle.dumps((args, kwargs))


@pytest.fixture(scope="function", autouse=True)
def render_template_to_json():
    with patch.object(web, "render_template") as mock_render_template:
        mock_render_template.side_effect = render_pickle
        yield


TESTCASES: Any = [({}, {"cols": [("", ColumnType.TEXT)]})]


@pytest.mark.parametrize("query, kwargs", TESTCASES)
def test_new_blank_table(client, query, kwargs):
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
    assert resp.headers["Location"] == f"http://localhost/{username}/{table_name}"


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
    assert (
        resp.headers["Location"]
        == f"http://localhost/{test_user.username}/{table_name}"
    )
