from io import BytesIO, SEEK_END
from datetime import date, datetime
from unittest.mock import ANY

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from csvbase import svc
from csvbase.value_objs import ContentType
from csvbase.web.func import set_current_user

from .conftest import ROMAN_NUMERALS
from . import utils


@pytest.fixture(
    scope="module", params=[ContentType.JSON, ContentType.HTML, ContentType.CSV]
)
def content_type(request):
    yield request.param


def test_create__happy(client, test_user):
    new_csv = """a,b,c,d,e
hello,1,1.5,FALSE,2018-01-03
"""
    table_name = utils.random_string()
    url = f"/{test_user.username}/{table_name}"
    resp = client.put(
        url,
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 201

    resp = client.get(
        url, headers={"Authorization": test_user.basic_auth(), "Accept": "text/csv"}
    )
    expected_df = (
        pd.read_csv(BytesIO(new_csv.encode("utf-8")))
        .assign(csvbase_row_id=[1])
        .set_index("csvbase_row_id")
    )
    actual_df = pd.read_csv(BytesIO(resp.data), index_col="csvbase_row_id")
    assert_frame_equal(expected_df, actual_df)


def test_create__public(client, test_user):
    new_csv = """a,b,c,d,e
hello,1,1.5,FALSE,2018-01-03
"""
    """Provide a way to set the public/private status for creation only."""
    table_name = utils.random_string()
    url = f"/{test_user.username}/{table_name}?public=yes"
    resp = client.put(
        url,
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 201

    get_resp = client.get(f"/{test_user.username}/{table_name}.json")
    assert get_resp.json["is_public"], "not public"


def test_create__invalid_name(client, test_user):
    new_csv = """a,b,c,d,e
hello,1,1.5,FALSE,2018-01-03
"""
    table_name = "some table"
    resp = client.put(
        f"/{test_user.username}/{table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that table name is invalid"}


def test_create__with_a_blank_csv(client, test_user):
    new_csv = ""

    table_name = "some-table"
    resp = client.put(
        f"/{test_user.username}/{table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that csv file is blank"}


def test_create__blank_column_name(client, test_user):
    new_csv = """a,,c
1,2,3"""

    table_name = "some-table"
    resp = client.put(
        f"/{test_user.username}/{table_name}?public=yes",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 201, resp.data

    get_resp = client.get(f"/{test_user.username}/{table_name}.parquet?public=yes")
    df = pd.read_parquet(BytesIO(get_resp.data))
    assert list(df.columns) == ["csvbase_row_id", "a", "col2", "c"]


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
    etag = first_resp.headers.get("ETag", None)

    first_cc = first_resp.cache_control
    if content_type == ContentType.HTML:
        assert etag is None
        assert first_cc.max_age == 0
        assert first_cc.private
    else:
        assert etag.startswith("W/")
        assert first_cc.max_age == 0
        assert not first_cc.private

    second_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value, "If-None-Match": etag},
    )
    if content_type == ContentType.HTML:
        assert second_resp.status_code == 200
    else:
        assert second_resp.status_code == 304
        # You're obliged to send the ETag with the 304
        second_etag = second_resp.headers["ETag"]
        assert second_etag == etag


def test_read__etag_cache_miss(client, ten_rows, test_user, content_type):
    if content_type == ContentType.HTML:
        pytest.skip("not relevant for html")
    first_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value, "If-None-Match": "wrong etag"},
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    etag = first_resp.headers["ETag"]

    assert etag.startswith("W/")


def test_read__last_changed_updates_the_etag(
    client, ten_rows, test_user, content_type, sesh
):
    if content_type == ContentType.HTML:
        pytest.skip("not relevant for html")
    first_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value},
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    first_etag = first_resp.headers["ETag"]

    svc.mark_table_changed(sesh, ten_rows.table_uuid)
    sesh.commit()

    second_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value},
    )
    assert second_resp.status_code == 200, second_resp.data
    assert content_type.value in second_resp.headers["Content-Type"]
    second_etag = second_resp.headers["ETag"]

    assert first_etag != second_etag


def test_read__metadata_headers(client, ten_rows, test_user, content_type, sesh):
    """Check that Last-Modified and Link headers are there - these are useful to consumers.

    It's also possible that the Link header helps search engines work out that
    tables with file extensions are duplicates of the un-extensioned path.

    """
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value},
    )
    assert (
        resp.headers.get("Link")
        == f'<http://localhost/{test_user.username}/{ten_rows.table_name}>, rel="canonical"'
    )
    # HTTP's format doesn't go to the microsecond level
    assert resp.last_modified == ten_rows.last_changed.replace(microsecond=0)


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
    if content_type == ContentType.HTML:
        assert content_type.value in resp.headers["Content-Type"]
    else:
        assert ContentType.JSON.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that table does not exist"}


def test_read__user_does_not_exist(client, test_user, content_type):
    resp = client.get("/someone/something", headers={"Accept": content_type.value})
    assert resp.status_code == 404, resp.data
    if content_type == ContentType.HTML:
        assert content_type.value in resp.headers["Content-Type"]
    else:
        assert ContentType.JSON.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that user does not exist"}


def test_read__is_private_not_authed(client, private_table, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 404, resp.data
    if content_type == ContentType.HTML:
        assert content_type.value in resp.headers["Content-Type"]
    else:
        assert ContentType.JSON.value in resp.headers["Content-Type"]
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
    if content_type in [ContentType.HTML, ContentType.JSON]:
        assert resp.status_code == 404, resp.data
    else:
        # FIXME: perhaps params should not be ignored for csv?
        assert resp.status_code == 200
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that page does not exist"}


def test_overwrite__no_ids(client, test_user, ten_rows):
    new_csv = """csvbase_row_id,roman_numeral,is_even,as_date,as_float
,X,yes,2018-01-10,10.0
,XI,no,2018-01-11,11.0
"""
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Content-Type": "text/csv", "Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200

    get_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}", headers={"Accept": "text/csv"}
    )
    df = pd.read_csv(BytesIO(get_resp.data))
    assert len(df) == 2


def test_overwrite__some_ids(client, test_user, ten_rows):
    url = f"/{test_user.username}/{ten_rows.table_name}"
    get_resp = client.get(url)

    buf = BytesIO(get_resp.data)
    buf.seek(0, SEEK_END)
    buf.write(
        b""",XI,no,2018-01-11,11.0
,XII,yes,2018-01-12,12.0
"""
    )
    buf.seek(0)

    post_resp = client.put(
        url,
        data=buf.getvalue(),
        headers={"Content-Type": "text/csv", "Authorization": test_user.basic_auth()},
    )
    assert post_resp.status_code == 200

    new_get_resp = client.get(url)
    df = pd.read_csv(BytesIO(new_get_resp.data))
    assert list(df.csvbase_row_id) == list(range(1, 13))


def test_overwrite__no_content_type(client, test_user, ten_rows):
    new_csv = """csvbase_row_id,roman_numeral,is_even,as_date,as_float
,X,yes,2018-01-10,10.0
,XI,no,2018-01-11,11.0
"""
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200


def test_overwrite__without_csvbase_row_id_column(client, test_user, ten_rows):
    new_csv = """roman_numeral,is_even,as_date,as_float
X,yes,2018-01-10,10.0
XI,no,2018-01-11,11.0
"""

    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200


@pytest.mark.xfail(reason="not implemented")
def test_overwrite__wrong_content_type(client, test_user, ten_rows):
    assert False


def test_overwrite__csv_header_doesnt_match(client, test_user, ten_rows):
    new_csv = """a,b,c
"""
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 400
    assert resp.json == {"error": "columns or types don't match existing"}


def test_overwrite__with_blank_no_headers(client, test_user, ten_rows):
    """Sending a just blank csv is not allowed, partly because that is a very
    common curl error.  "Truncate" is done with a single csv with just the
    header.

    """
    new_csv = ""
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 400
    assert resp.json == {"error": "that csv file is blank"}


def test_overwrite__with_just_header(client, test_user, ten_rows):
    new_csv = """roman_numeral,is_even,as_date,as_float
"""
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200
    get_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}.parquet",
    )
    df = pd.read_parquet(BytesIO(get_resp.data))
    assert len(df) == 0


def test_append__happy(client, test_user, ten_rows):
    new_csv = """roman_numeral,is_even,as_date,as_float
XI,no,2018-01-11,11.0
XII,yes,2018-01-12,12.0
XIII,no,2018-01-13,13.0
XIV,yes,2018-01-14,14.0
XV,no,2018-01-15,15.0
"""
    resp = client.post(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 204

    get_resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}", headers={"Accept": "text/csv"}
    )
    df = pd.read_csv(BytesIO(get_resp.data))
    assert len(df) == 15


def test_delete__happy(client, test_user, ten_rows):
    set_current_user(test_user)
    resp = client.delete(f"{test_user.username}/{ten_rows.table_name}")
    assert resp.status_code == 204


def test_delete_via_post__happy(client, test_user, ten_rows):
    set_current_user(test_user)
    resp = client.post(
        f"{test_user.username}/{ten_rows.table_name}/delete-table-form-post"
    )
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}"


def test_delete_via_post__has_readme(sesh, client, test_user, ten_rows):
    set_current_user(test_user)
    svc.set_readme_markdown(sesh, test_user.user_uuid, ten_rows.table_name, "something")
    sesh.commit()
    resp = client.post(
        f"{test_user.username}/{ten_rows.table_name}/delete-table-form-post"
    )
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}"


@pytest.mark.parametrize("is_public", [True, False])
def test_delete_via_post__am_authed(sesh, client, test_user, is_public):
    set_current_user(test_user)
    table = utils.create_table(sesh, test_user, is_public=is_public)
    sesh.commit()

    set_current_user(test_user)
    resp = client.post(
        f"{test_user.username}/{table.table_name}/delete-table-form-post"
    )
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}"


@pytest.mark.parametrize("is_public", [True, False])
def test_delete_via_post__not_authed(sesh, client, test_user, is_public, crypt_context):
    table = utils.create_table(sesh, test_user, is_public=is_public)
    sesh.commit()

    set_current_user(utils.make_user(sesh, crypt_context))
    resp = client.post(
        f"{test_user.username}/{table.table_name}/delete-table-form-post"
    )
    assert resp.status_code == 401


@pytest.mark.parametrize("is_public", [True, False])
def test_delete_via_post__wrong_user(sesh, client, test_user, is_public, crypt_context):
    table = utils.create_table(sesh, test_user, is_public=is_public)
    sesh.commit()

    set_current_user(utils.make_user(sesh, crypt_context))
    resp = client.post(
        f"{test_user.username}/{table.table_name}/delete-table-form-post"
    )
    assert resp.status_code == 401


def test_delete_via_post__does_not_exist(client, test_user):
    set_current_user(test_user)
    resp = client.post(
        f"{test_user.username}/{utils.random_string()}/delete-table-form-post"
    )
    assert resp.status_code == 404
