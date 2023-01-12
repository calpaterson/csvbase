from unittest.mock import ANY

import pytest

from csvbase.value_objs import ContentType

from .conftest import ROMAN_NUMERALS
from . import utils


@pytest.fixture(scope="module", params=[ContentType.JSON, ContentType.HTML])
def content_type(request):
    yield request.param


def test_read__happy(client, ten_rows, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/{ten_rows}", headers={"Accept": content_type.value}
    )
    assert resp.status_code == 200, resp.data
    assert content_type.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        expected_page_dict = {
            "rows": [
                {
                    "row": {"roman_numeral": rn},
                    "row_id": index,
                    "url": f"http://localhost/{test_user.username}/{ten_rows}/rows/{index}",
                }
                for index, rn in enumerate(ROMAN_NUMERALS[:11], start=1)
            ],
            "next_page_url": None,
            "previous_page_url": None,
        }

        assert resp.json == {
            "name": ten_rows,
            "is_public": True,
            "caption": "Roman numerals",
            "data_licence": "All rights reserved",
            "columns": [
                {"name": "csvbase_row_id", "type": "integer"},
                {
                    "name": "roman_numeral",
                    # FIXME: is "string" the right name?
                    "type": "string",
                },
            ],
            "page": expected_page_dict,
            "approx_size": 10,
        }


def test_read__table_does_not_exist(client, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/something", headers={"Accept": content_type.value}
    )
    assert resp.status_code == 404, resp.data
    assert content_type.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that table does not exist"}


def test_read__user_does_not_exist(client, test_user, content_type):
    resp = client.get("/someone/something", headers={"Accept": content_type.value})
    assert resp.status_code == 404, resp.data
    assert content_type.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that user does not exist"}


def test_read__is_private_not_authed(client, private_table, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 404, resp.data
    assert content_type.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that table does not exist"}


@pytest.mark.xfail(reason="test not implemented", strict=True)
def test_read__is_private_am_authed(client, private_table, test_user):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200, resp.data
    assert resp.json == {"row_id": 1, "row": {"x": 1}}


def test_read__empty_table(sesh, client, test_user, content_type):
    table = utils.create_table(sesh, test_user, [])
    sesh.commit()
    resp = client.get(f"/{test_user.username}/{table.table_name}")
    assert resp.status_code == 200, resp.data


def test_read__paging_over_the_top(client, test_user, ten_rows, content_type):
    resp = client.get(
        f"/{test_user.username}/{ten_rows}",
        query_string={"op": "gt", "n": "10"},
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 404, resp.data
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that row does not exist"}
