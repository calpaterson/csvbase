from typing import Any
from unittest.mock import patch
import pickle

from flask import url_for
import pytest
from csvbase import web
from csvbase.value_objs import ColumnType


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
    assert template_kwargs == kwargs
