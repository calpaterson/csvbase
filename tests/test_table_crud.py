import pytest


@pytest.mark.xfail(reason="test not implemented", strict=True)
def test_read__happy(client, ten_rows, test_user):
    resp = client.get(f"/{test_user.username}/{ten_rows}")
    assert resp.status_code == 200, resp.data
    assert resp.json == {"row_id": 1, "row": {"roman_numeral": "I"}}


@pytest.mark.xfail(reason="test not implemented", strict=True)
def test_read__table_does_not_exist(client, test_user):
    resp = client.get(f"/{test_user.username}/something/")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "table does not exist"}


@pytest.mark.xfail(reason="test not implemented", strict=True)
def test_read__user_does_not_exist(client, test_user):
    resp = client.get("/someone/something/")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "user does not exist"}


@pytest.mark.xfail(reason="test not implemented", strict=True)
def test_read__is_private_not_authed(client, private_table, test_user):
    resp = client.get(f"/{test_user.username}/{private_table}")
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "table does not exist"}


@pytest.mark.xfail(reason="test not implemented", strict=True)
def test_read__is_private_am_authed(client, private_table, test_user):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200, resp.data
    assert resp.json == {"row_id": 1, "row": {"x": 1}}
