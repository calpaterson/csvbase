import re
from datetime import date, datetime
from unittest.mock import ANY

import pytest

from csvbase.value_objs import ContentType

from .conftest import ROMAN_NUMERALS
from . import utils


def test_read__happy(client, ten_rows, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 200, resp.data
    assert content_type.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        expected_page_dict = {
            "rows": [
                {
                    "row": {
                        "roman_numeral": rn,
                        "is_even": (index % 2) == 0,
                        "as_date": date(2018, 1, index).isoformat(),
                        "as_float": index + 0.5,
                    },
                    "row_id": index,
                    "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/{index}",
                }
                for index, rn in enumerate(ROMAN_NUMERALS[:11], start=1)
            ],
            "next_page_url": None,
            "previous_page_url": None,
        }

        actual_json = resp.json
        expected_json = {
            "name": ten_rows.table_name,
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
                {"name": "is_even", "type": "boolean"},
                {"name": "as_date", "type": "date"},
                {"name": "as_float", "type": "float"},
            ],
            "page": expected_page_dict,
            "approx_size": 10,
            "last_changed": ANY,
            "created": ANY,
        }
        assert actual_json == expected_json
        assert datetime.fromisoformat(actual_json["last_changed"]) is not None
        assert datetime.fromisoformat(actual_json["created"]) is not None


def test_read__etag_cache_hit(client, ten_rows, test_user, content_type):
    first_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value},
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    etag = first_resp.headers["ETag"]

    assert etag.startswith("W/")
    first_cc = first_resp.cache_control
    if content_type == ContentType.HTML:
        assert first_cc.no_cache
        assert first_cc.private
    else:
        assert first_cc.no_cache
        assert not first_cc.private

    second_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value, "If-None-Match": etag},
    )
    assert second_resp.status_code == 304
    # You're obliged to send the ETag with the 304
    second_etag = second_resp.headers["ETag"]
    assert second_etag == etag


def test_read__etag_cache_miss(client, ten_rows, test_user, content_type):
    first_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value, "If-None-Match": "wrong etag"},
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    etag = first_resp.headers["ETag"]

    assert etag.startswith("W/")


def test_read__with_no_rows(sesh, client, test_user, content_type):
    table = utils.create_table(sesh, test_user, [])
    sesh.commit()
    resp = client.get(
        f"/{test_user.username}/{table.table_name}",
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 200


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
        f"/{test_user.username}/{ten_rows.table_name}",
        query_string={"op": "gt", "n": "10"},
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 404, resp.data
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that page does not exist"}
