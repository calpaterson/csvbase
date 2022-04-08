import pytest

from csvbase.value_objs import ContentType


def test_read__happy(client, ten_rows, test_user):
    resp = client.get(
        f"/{test_user.username}/{ten_rows}", headers={"Accept": ContentType.JSON.value}
    )
    assert resp.status_code == 200, resp.data
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
    }


def test_read__table_does_not_exist(client, test_user):
    resp = client.get(
        f"/{test_user.username}/something", headers={"Accept": ContentType.JSON.value}
    )
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "table does not exist"}


def test_read__user_does_not_exist(client, test_user):
    resp = client.get("/someone/something", headers={"Accept": ContentType.JSON.value})
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "user does not exist"}


def test_read__is_private_not_authed(client, private_table, test_user):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Accept": ContentType.JSON.value},
    )
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
