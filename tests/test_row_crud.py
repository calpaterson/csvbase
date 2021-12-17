from csvbase import svc
from csvbase.value_objs import Column, ColumnType
from .utils import random_string

import pytest

from .utils import make_user

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


@pytest.fixture(scope="module")
def private_table(test_user, module_sesh):
    table_name = random_string()
    svc.upsert_table_metadata(
        module_sesh, test_user.user_uuid, table_name, public=False
    )
    svc.create_table(
        module_sesh,
        test_user.username,
        table_name,
        [Column("x", type_=ColumnType.INTEGER)],
    )
    svc.insert_row(module_sesh, test_user.username, table_name, {"x": 1})
    module_sesh.commit()
    return table_name


def test_create__happy(client, ten_rows, test_user):
    expected_resource = {"row_id": 11, "row": {"roman_numeral": "XI"}}

    post_resp = client.post(
        f"/{test_user.username}/{ten_rows}/rows/",
        json={"row": {"roman_numeral": "XI"}},
        headers={"Authorization": test_user.basic_auth()},
    )
    assert post_resp.status_code == 201
    assert post_resp.json == expected_resource

    get_resp = client.get(f"/{test_user.username}/{ten_rows}/rows/11")
    assert get_resp.status_code == 200, get_resp.data
    assert get_resp.json == post_resp.json


@pytest.mark.xfail(reason="not implemented")
def test_create__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_create__user_does_not_exist(client, test_user):
    assert False


def test_create__not_authed(client, ten_rows, test_user):
    post_resp = client.post(
        f"/{test_user.username}/{ten_rows}/rows/",
        json={"row": {"roman_numeral": "XI"}},
    )
    assert post_resp.status_code == 401
    assert post_resp.json == {"error": "not authenticated"}


@pytest.mark.xfail(reason="not implemented")
def test_create__not_authed_private_table(client, private_table, test_user):
    assert False


def test_create__wrong_user(client, ten_rows, test_user, app, sesh):
    other_user = make_user(sesh, app.config["CRYPT_CONTEXT"])
    sesh.commit()
    resp = client.post(
        f"/{test_user.username}/{ten_rows}/rows/",
        json={"row": {"roman_numeral": "XI"}},
        headers={"Authorization": other_user.basic_auth()},
    )
    assert resp.status_code == 403, resp.data
    assert resp.json == {"error": "not allowed"}


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


def test_read__user_does_not_exist(client, test_user):
    resp = client.get("/someone/something/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "user does not exist"}


def test_read__is_private_not_authed(client, private_table, test_user):
    resp = client.get(f"/{test_user.username}/{private_table}/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "table does not exist"}


def test_read__is_private_am_authed(client, private_table, test_user):
    resp = client.get(
        f"/{test_user.username}/{private_table}/rows/1",
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200, resp.data
    assert resp.json == {"row_id": 1, "row": {"x": 1}}


def test_update__happy(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows}/rows/1"
    new = {"row_id": 1, "row": {"roman_numeral": "i"}}
    resp = client.put(url, json=new)
    assert resp.status_code == 204, resp.data

    resp = client.get(url)
    assert resp.json == new


def test_update__row_does_not_exist(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows}/rows/11"
    new = {"row_id": 11, "row": {"roman_numeral": "XI"}}
    resp = client.put(url, json=new)
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "row does not exist"}


@pytest.mark.xfail(reason="not implemented")
def test_update__row_id_does_not_match(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_update__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_update__user_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_update__is_private_not_authed(client, private_table, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_update__is_private_am_authed(client, private_table, test_user):
    assert False


def test_delete__happy(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows}/rows/1"
    resp = client.delete(url, headers={"Authorization": test_user.basic_auth()})
    assert resp.status_code == 204, resp.data

    resp = client.get(url)
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "row does not exist"}


@pytest.mark.xfail(reason="not implemented")
def test_delete__row_does_not_exist(client, ten_rows, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_delete__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_delete__user_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_delete__is_private_not_authed(client, private_table, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_delete__is_private_am_authed(client, private_table, test_user):
    assert False
