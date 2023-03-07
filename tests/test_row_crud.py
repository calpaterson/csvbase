import pytest

from .utils import make_user


def test_create__happy(client, ten_rows, test_user):
    expected_resource = {
        "row_id": 11,
        "row": {
            "roman_numeral": "XI",
            "is_even": False,
            "as_date": "2018-01-11",
            "as_float": 11.5,
        },
        "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/11",
    }

    post_resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/",
        json={
            "row": {
                "roman_numeral": "XI",
                "is_even": False,
                "as_date": "2018-01-11",
                "as_float": 11.5,
            }
        },
        headers={"Authorization": test_user.basic_auth()},
    )
    assert post_resp.status_code == 201
    assert post_resp.json == expected_resource

    get_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/rows/11")
    assert get_resp.status_code == 200, get_resp.data
    assert get_resp.json == expected_resource


def test_create__table_does_not_exist(client, test_user):
    post_resp = client.post(
        f"/{test_user.username}/ten_rows/rows/",
        json={"row": {"roman_numeral": "XI"}},
        headers={"Authorization": test_user.basic_auth()},
    )
    assert post_resp.status_code == 404
    assert post_resp.json == {"error": "that table does not exist"}


def test_create__user_does_not_exist(client, test_user):
    post_resp = client.post(
        "/somebody/ten_rows/rows/",
        json={"row": {"roman_numeral": "XI"}},
        headers={"Authorization": test_user.basic_auth()},
    )
    assert post_resp.status_code == 404
    assert post_resp.json == {"error": "that user does not exist"}


def test_create__not_authed(client, ten_rows, test_user):
    post_resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/",
        json={"row": {"roman_numeral": "XI"}},
    )
    assert post_resp.status_code == 401
    assert post_resp.json == {"error": "you need to sign in to do that"}


@pytest.mark.xfail(reason="test not implemented")
def test_create__is_private_not_authed(client, private_table, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_create__is_private_am_authed(client, private_table, test_user):
    assert False


def test_create__wrong_user(client, ten_rows, test_user, app, sesh):
    other_user = make_user(sesh, app.config["CRYPT_CONTEXT"])
    sesh.commit()
    resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/",
        json={"row": {"roman_numeral": "XI"}},
        headers={"Authorization": other_user.basic_auth()},
    )
    assert resp.status_code == 403, resp.data
    assert resp.json == {"error": "that's not allowed"}


def test_read__happy(client, ten_rows, test_user):
    resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/rows/1")
    assert resp.status_code == 200, resp.data
    assert resp.json == {
        "row_id": 1,
        "row": {
            "roman_numeral": "I",
            "is_even": False,
            "as_date": "2018-01-01",
            "as_float": 1.5,
        },
        "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/1",
    }


def test_read__row_does_not_exist(client, ten_rows, test_user):
    resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/rows/11")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that row does not exist"}


def test_read__table_does_not_exist(client, test_user):
    resp = client.get(f"/{test_user.username}/something/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that table does not exist"}


def test_read__user_does_not_exist(client, test_user):
    resp = client.get("/someone/something/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that user does not exist"}


def test_read__is_private_not_authed(client, private_table, test_user):
    resp = client.get(f"/{test_user.username}/{private_table}/rows/1")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that table does not exist"}


def test_read__is_private_am_authed(client, private_table, test_user):
    resp = client.get(
        f"/{test_user.username}/{private_table}/rows/1",
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200, resp.data
    assert resp.json == {
        "row_id": 1,
        "row": {"x": 1},
        "url": f"http://localhost/{test_user.username}/{private_table}/rows/1",
    }


def test_update__happy(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    new = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-01-01",
            "as_float": 1.5,
        },
        "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/1",
    }
    resp = client.put(url, json=new)
    assert resp.status_code == 200, resp.data
    assert resp.json == new

    resp = client.get(url)
    assert resp.json == new


def test_update__row_does_not_exist(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/11"
    new = {
        "row_id": 11,
        "row": {
            "roman_numeral": "XI",
            "is_even": True,
            "as_date": "2018-01-01",
            "as_float": 1.5,
        },
    }
    resp = client.put(url, json=new)
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that row does not exist"}


@pytest.mark.xfail(reason="test (and functionality!) not implemented")
def test_update__row_does_not_match():
    """Test that where the row columns are wrong you get some kind of 4xx error"""


def test_update__row_id_does_not_match(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/10"
    new = {"row_id": 11, "row": {"roman_numeral": "X+1"}}
    resp = client.put(url, json=new)
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "invalid request"}


@pytest.mark.xfail(reason="test not implemented")
def test_update__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_update__user_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_update__is_private_not_authed(client, private_table, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_update__is_private_am_authed(client, private_table, test_user):
    assert False


def test_delete__happy(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    resp = client.delete(url, headers={"Authorization": test_user.basic_auth()})
    assert resp.status_code == 204, resp.data
    assert resp.data == b""

    resp = client.get(url)
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that row does not exist"}


@pytest.mark.xfail(reason="test not implemented")
def test_delete__row_does_not_exist(client, ten_rows, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_delete__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_delete__user_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_delete__is_private_not_authed(client, private_table, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_delete__is_private_am_authed(client, private_table, test_user):
    assert False
