from typing import Any, Dict
from csvbase.web.func import set_current_user
from datetime import datetime
import pytest

from csvbase.value_objs import ContentType
from csvbase.userdata import PGUserdataAdapter
from .utils import make_user, assert_is_valid_etag, create_table


@pytest.fixture(scope="module", params=[ContentType.JSON, ContentType.HTML_FORM])
def post_content_type(request):
    yield request.param


@pytest.fixture(scope="module", params=[ContentType.JSON, ContentType.HTML])
def accept_content_type(request):
    # Rows only really support JSON and HTML
    yield request.param


def test_create__happy(client, ten_rows, test_user, accept_content_type):
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
        headers={
            "Authorization": test_user.basic_auth(),
            "Accept": accept_content_type.value,
        },
    )
    if accept_content_type == ContentType.HTML:
        assert post_resp.status_code == 302
        assert (
            post_resp.headers["Location"]
            == f"/{test_user.username}/{ten_rows.table_name}?n=12&op=lt&highlight=11"
        )
    elif accept_content_type == ContentType.JSON:
        assert post_resp.status_code == 201
        assert post_resp.json == expected_resource

        row_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/rows/11")
        assert row_resp.status_code == 200, row_resp.data
        assert row_resp.json == expected_resource

        table_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}.json")
        assert (
            datetime.fromisoformat(table_resp.json["last_changed"])
            > ten_rows.last_changed
        )


def test_create__with_row_id(client, ten_rows, test_user, accept_content_type):
    """Creating a row with a specific row id is (currently) forbidden"""
    set_current_user(test_user)
    post_resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/",
        json={
            "row_id": 1,
            "row": {
                "roman_numeral": "XI",
                "is_even": False,
                "as_date": "2018-01-11",
                "as_float": 11.5,
            },
        },
        headers={
            "Accept": accept_content_type.value,
        },
    )
    assert post_resp.status_code == 400


def test_create__wrong_type(
    client, ten_rows, test_user, accept_content_type, post_content_type
):
    """Test that creating a row with the wrong content type generates some kind of 400 error"""
    set_current_user(test_user)
    kwargs = {"headers": {"Accept": accept_content_type.value}}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = {
            "row": {
                "roman_numeral": "XI",
                "is_even": False,
                "as_date": "2018-01-11",
                "as_float": "jake",
            }
        }
    else:
        kwargs["data"] = {
            "roman_numeral": "XI",
            "is_even": "",
            "as_date": "2018-01-11",
            "as_float": "jake",
        }

    post_resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/", **kwargs
    )
    assert post_resp.status_code == 422
    if accept_content_type == ContentType.JSON:
        assert post_resp.json == {
            "error": "unable to convert the data you provided to the required type"
        }


def test_create__missing_column(
    client, ten_rows, test_user, accept_content_type, post_content_type
):
    set_current_user(test_user)
    kwargs = {"headers": {"Accept": accept_content_type.value}}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = {
            "row": {
                "roman_numeral": "XI",
                "is_even": False,
                "as_date": "2018-01-11",
            }
        }
    else:
        kwargs["data"] = {
            "roman_numeral": "XI",
            "is_even": "",
            "as_date": "2018-01-11",
        }

    post_resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/", **kwargs
    )
    if accept_content_type == ContentType.JSON:
        assert post_resp.status_code == 201
    else:
        assert post_resp.status_code == 302

    get_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/rows/11")
    expected = {
        "row_id": 11,
        "row": {
            "roman_numeral": "XI",
            "is_even": False,
            "as_date": "2018-01-11",
            "as_float": None,
        },
        "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/11",
    }
    assert get_resp.json == expected


def test_create__extra_column(
    client, ten_rows, test_user, accept_content_type, post_content_type
):
    set_current_user(test_user)
    kwargs = {"headers": {"Accept": accept_content_type.value}}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = {
            "row": {
                "roman_numeral": "XI",
                "is_even": False,
                "as_date": "2018-01-11",
                "as_float": 1.5,
                "extra": "kings",
            }
        }
    else:
        kwargs["data"] = {
            "roman_numeral": "XI",
            "is_even": "",
            "as_date": "2018-01-11",
            "as_float": "1.5",
            "extra": "kings",
        }

    post_resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}/rows/", **kwargs
    )
    assert post_resp.status_code == 400
    if accept_content_type == ContentType.JSON:
        assert post_resp.json == {"error": "columns or types don't match existing"}


def test_create__table_does_not_exist(client, test_user):
    post_resp = client.post(
        f"/{test_user.username}/ten-rows/rows/",
        json={"row": {"roman_numeral": "XI"}},
        headers={"Authorization": test_user.basic_auth()},
    )
    assert post_resp.status_code == 404
    assert post_resp.json == {"error": "that table does not exist"}


def test_create__user_does_not_exist(client, test_user):
    post_resp = client.post(
        "/somebody/ten-rows/rows/",
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


def test_read__happy(client, ten_rows, test_user, accept_content_type):
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}/rows/1",
        headers={"Accept": accept_content_type.value},
    )
    assert resp.status_code == 200, resp.data

    # test that the cache headers are as expected
    cc_obj = resp.cache_control
    vary = resp.headers.get("Vary")
    assert cc_obj.no_cache
    assert cc_obj.must_revalidate
    assert vary == "Accept, Cookie"

    if accept_content_type is ContentType.JSON:
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


def test_read__no_accept(client, ten_rows, test_user):
    # Checks that JSON gets content negotiated by default
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}/rows/1",
    )
    assert resp.status_code == 200, resp.data
    assert resp.headers["Content-Type"] == ContentType.JSON.value
    assert resp.json["row_id"] == 1


@pytest.mark.xfail(reason="not implemented")
def test_read__etag_cache_hit():
    assert False


def test_read__etag_cache_miss(client, ten_rows, test_user, accept_content_type):
    first_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}/rows/1",
        headers={"Accept": accept_content_type.value, "If-None-Match": 'W/"rong etag"'},
    )
    assert first_resp.status_code == 200, first_resp.data
    assert accept_content_type.value in first_resp.headers["Content-Type"]
    etag = first_resp.headers["ETag"]

    assert_is_valid_etag(etag)


@pytest.mark.xfail(reason="not implemented")
def test_read__changed_data_changes_the_etag():
    assert False


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


def test_read__is_private_am_authed(
    client, private_table, test_user, accept_content_type
):
    resp = client.get(
        f"/{test_user.username}/{private_table}/rows/1",
        headers={
            "Authorization": test_user.basic_auth(),
            "Accept": accept_content_type.value,
        },
    )
    assert resp.status_code == 200, resp.data

    cc_obj = resp.cache_control
    assert cc_obj.private, "Cache-Control does not include 'private'"
    assert cc_obj.no_cache

    if accept_content_type is ContentType.JSON:
        assert resp.json == {
            "row_id": 1,
            "row": {"x": 1},
            "url": f"http://localhost/{test_user.username}/{private_table}/rows/1",
        }


@pytest.mark.parametrize("is_public", [True, False])
def test_read__as_another_user(
    client, is_public, test_user, accept_content_type, crypt_context, sesh
):
    table = create_table(sesh, test_user, is_public=is_public)
    other_user = make_user(sesh, crypt_context)
    sesh.commit()

    set_current_user(other_user)
    resp = client.get(
        f"/{test_user.username}/{table.table_name}/rows/1",
        headers={"Accept": accept_content_type.value},
    )
    if not is_public:
        resp.status_code == 404
    else:
        resp.status_code == 200


POST_CONTENT_TYPE_TO_VERB = {ContentType.HTML_FORM: "POST", ContentType.JSON: "PUT"}


def test_update__happy(client, ten_rows, test_user, post_content_type):
    """Test updating via POST(HTML, browser)/PUT(JSON, client)"""
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    set_current_user(test_user)
    json_body = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
            "as_float": 1.5,
        },
        "url": f"http://localhost{url}",
    }
    verb = POST_CONTENT_TYPE_TO_VERB[post_content_type]
    kwargs: Dict[str, Any] = {"method": verb}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = json_body
    else:
        form = {
            "csvbase_row_id": 1,
            "roman_numeral": "i",
            "is_even": "on",
            "as_date": "2018-02-01",
            "as_float": "1.5",
        }
        kwargs["data"] = form
    resp = client.open(url, **kwargs)
    if post_content_type is ContentType.JSON:
        assert resp.status_code == 200, resp.data
        assert resp.json == json_body
    else:
        assert resp.status_code == 302, resp.data

    resp = client.get(url)
    assert resp.json == json_body

    table_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}.json")
    assert (
        datetime.fromisoformat(table_resp.json["last_changed"]) > ten_rows.last_changed
    )


def test_update__missing_row_id(client, ten_rows, test_user, post_content_type):
    """Callers must include the row id for updates, and at the moment, it must match."""
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    set_current_user(test_user)
    json_body = {
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
        },
        "url": f"http://localhost{url}",
    }
    verb = POST_CONTENT_TYPE_TO_VERB[post_content_type]
    kwargs: Dict[str, Any] = {"method": verb}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = json_body
    else:
        form = {
            "roman_numeral": "i",
            "is_even": "on",
            "as_date": "2018-02-01",
        }
        kwargs["data"] = form
    resp = client.open(url, **kwargs)
    assert resp.status_code == 400


def test_update__missing_column(client, ten_rows, test_user, post_content_type):
    """Check that missing columns are allowed, but come through as None/NULL"""
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    set_current_user(test_user)
    json_body = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
        },
        "url": f"http://localhost{url}",
    }
    verb = POST_CONTENT_TYPE_TO_VERB[post_content_type]
    kwargs: Dict[str, Any] = {"method": verb}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = json_body
    else:
        form = {
            "csvbase_row_id": 1,
            "roman_numeral": "i",
            "is_even": "on",
            "as_date": "2018-02-01",
        }
        kwargs["data"] = form
    resp = client.open(url, **kwargs)
    if post_content_type is ContentType.JSON:
        assert resp.status_code == 200, resp.data
        assert resp.json == json_body
    else:
        assert resp.status_code == 302, resp.data

    expected = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
            "as_float": None,
        },
        "url": f"http://localhost{url}",
    }
    resp = client.get(url)
    assert resp.json == expected

    table_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}.json")
    assert (
        datetime.fromisoformat(table_resp.json["last_changed"]) > ten_rows.last_changed
    )


def test_update__extra_column(client, ten_rows, test_user, post_content_type):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    set_current_user(test_user)
    json_body = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
            "extra": "kings",
        },
        "url": f"http://localhost{url}",
    }
    verb = POST_CONTENT_TYPE_TO_VERB[post_content_type]
    kwargs: Dict[str, Any] = {"method": verb}
    if post_content_type is ContentType.JSON:
        kwargs["json"] = json_body
    else:
        form = {
            "csvbase_row_id": 1,
            "roman_numeral": "i",
            "is_even": "on",
            "as_date": "2018-02-01",
            "extra": "kings",
        }
        kwargs["data"] = form
    resp = client.open(url, **kwargs)
    assert resp.status_code == 400, resp.data


@pytest.mark.xfail(reason="not implemented")
def test_update__etag_matches(client, test_user, ten_rows):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_update__etag_doesnt_match(client, test_user, ten_rows):
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        json={
            "row_id": 1,
            "row": {
                "roman_numeral": "i",
                "is_even": True,
                "as_date": "2018-02-01",
                "as_float": 1.5,
            },
            "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/1",
        },
        headers={},
    )


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
    resp = client.put(url, json=new, headers={"Authorization": test_user.basic_auth()})
    assert resp.status_code == 404, resp.data
    assert resp.json == {"error": "that row does not exist"}


@pytest.mark.xfail(reason="test (and functionality!) not implemented")
def test_update__row_does_not_match():
    """Test that where the row columns are wrong you get some kind of 4xx error"""
    assert False


def test_update__row_id_does_not_match(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/10"
    new = {"row_id": 11, "row": {"roman_numeral": "X+1"}}
    resp = client.put(url, json=new, headers={"Authorization": test_user.basic_auth()})
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "invalid request"}


@pytest.mark.xfail(reason="test not implemented")
def test_update__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_update__user_does_not_exist(client, test_user):
    assert False


def test_update__is_public_not_authed(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1"
    new = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
            "as_float": 1.5,
        },
        "url": f"http://localhost/{test_user.username}/{ten_rows.table_name}/rows/1",
    }
    resp = client.put(url, json=new)
    assert resp.status_code == 401, resp.data


def test_update__is_private_not_authed(client, private_table, test_user):
    url = f"/{test_user.username}/{private_table}/rows/1"
    new = {
        "row_id": 1,
        "row": {
            "roman_numeral": "i",
            "is_even": True,
            "as_date": "2018-02-01",
            "as_float": 1.5,
        },
        "url": f"http://localhost/{test_user.username}/{private_table}/rows/1",
    }
    resp = client.put(url, json=new)
    assert resp.status_code == 404, resp.data


@pytest.mark.xfail(reason="test not implemented")
def test_update__is_private_am_authed(client, private_table, test_user):
    assert False


@pytest.mark.parametrize("is_public", [True, False])
def test_update__as_another_user(client, is_public, test_user, crypt_context, sesh):
    table = create_table(sesh, test_user, is_public=is_public)
    other_user = make_user(sesh, crypt_context)
    sesh.commit()

    new = {"row_id": 1, "row": {"a": 2}}

    set_current_user(other_user)
    resp = client.put(f"/{test_user.username}/{table.table_name}/rows/1", json=new)
    if not is_public:
        resp.status_code == 403
    else:
        resp.status_code == 401


@pytest.fixture(
    scope="module",
    params=[
        pytest.param(
            ("{username}/{table_name}/rows/{row_id}", "DELETE"), id="delete via api"
        ),
        pytest.param(
            ("{username}/{table_name}/rows/{row_id}/delete-row-for-browsers", "POST"),
            id="delete via form post",
        ),
    ],
)
def delete_mode(request):
    yield request.param


@pytest.mark.parametrize("is_public", [True, False])
def test_delete__am_authed(client, ten_rows, test_user, is_public, delete_mode):
    set_current_user(test_user)
    url_template, verb = delete_mode
    url = url_template.format(
        username=test_user.username, table_name=ten_rows.table_name, row_id=1
    )
    resp = client.open(url, method=verb)
    if verb == "POST":
        assert resp.status_code == 302
    else:
        assert resp.status_code == 204, resp.data
        assert resp.data == b""

    if verb == "DELETE":
        # FIXME: check this for POST as well
        resp = client.get(url)
        assert resp.status_code == 404, resp.data
        assert resp.json == {"error": "that row does not exist"}

    table_resp = client.get(f"/{test_user.username}/{ten_rows.table_name}.json")
    assert (
        datetime.fromisoformat(table_resp.json["last_changed"]) > ten_rows.last_changed
    )


@pytest.mark.parametrize("is_public", [True, False])
def test_delete__not_authed(sesh, client, test_user, is_public, delete_mode):
    table = create_table(sesh, test_user, is_public=is_public)
    backend = PGUserdataAdapter(sesh)
    backend.insert_row(
        table.table_uuid,
        backend.get_a_sample_row(table.table_uuid),
    )
    sesh.commit()
    url_template, verb = delete_mode

    resp = client.open(
        url_template.format(
            username=test_user.username, table_name=table.table_name, row_id=1
        ),
        method=verb,
    )
    if is_public:
        assert resp.status_code == 401
    else:
        assert resp.status_code == 404
        assert resp.json == {"error": "that table does not exist"}


@pytest.mark.parametrize("is_public", [True, False])
def test_delete__wrong_user(sesh, client, test_user, is_public, delete_mode):
    table = create_table(sesh, test_user, is_public=is_public)
    backend = PGUserdataAdapter(sesh)
    backend.insert_row(
        table.table_uuid,
        backend.get_a_sample_row(table.table_uuid),
    )
    sesh.commit()
    url_template, verb = delete_mode

    resp = client.open(
        url_template.format(
            username=test_user.username, table_name=table.table_name, row_id=1
        ),
        method=verb,
    )
    if is_public:
        assert resp.status_code == 401
    else:
        assert resp.status_code == 404
        assert resp.json == {"error": "that table does not exist"}


@pytest.mark.xfail(reason="test not implemented")
def test_delete__row_does_not_exist(client, ten_rows, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_delete__table_does_not_exist(client, test_user):
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_delete__user_does_not_exist(client, test_user):
    assert False


def test_delete__html_row_delete_check(client, ten_rows, test_user):
    url = f"/{test_user.username}/{ten_rows.table_name}/rows/1/delete-check"
    set_current_user(test_user)
    resp = client.get(url)
    assert resp.status_code == 200
