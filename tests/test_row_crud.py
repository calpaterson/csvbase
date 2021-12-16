from csvbase import svc
from csvbase.value_objs import Column, ColumnType
from .utils import random_string

import pytest

ROMAN_NUMERALS = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]


@pytest.fixture(scope="function")
def ten_rows(test_user, sesh):
    table_name = random_string()
    svc.upsert_table_metadata(sesh, test_user.user_uuid, table_name, public=True)
    svc.create_table(
        sesh,
        test_user.username,
        table_name,
        [Column("roman_numeral", type_=ColumnType.TEXT)],
    )
    for roman_numeral in ROMAN_NUMERALS:
        svc.insert_row(
            sesh, test_user.username, table_name, {"roman_numeral": roman_numeral}
        )
    sesh.commit()
    return table_name


@pytest.mark.xfail(reason="not implemented")
def test_create(client, ten_rows):
    assert False


def test_read__happy(client, ten_rows, test_user):
    resp = client.get(f"/{test_user.username}/{ten_rows}/rows/1")
    assert resp.status_code == 200, resp.data
    assert resp.json == {"row_id": 1, "row": {"roman_numeral": "I"}}


def test_read__row_does_not_exist(client, ten_rows, test_user):
    resp = client.get(f"/{test_user.username}/{ten_rows}/rows/11")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "row does not exist"}


def test_read__table_does_not_exist(client, test_user):
    resp = client.get(f"/{test_user.username}/something/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "table does not exist"}


@pytest.mark.xfail(reason="not implemented")
def test_read__user_does_not_exist(client, test_user):
    resp = client.get(f"/{test_user.username}/something/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "user does not exist"}


@pytest.mark.xfail(reason="not implemented")
def test_read__is_private(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_update(client, ten_rows):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_delete(client, ten_rows):
    assert False
