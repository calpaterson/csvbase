from io import BytesIO, SEEK_END, StringIO
from datetime import date, datetime, timezone
from unittest.mock import ANY
from typing import Mapping, Optional
from urllib.parse import quote_plus
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from werkzeug.wrappers.response import Response

from csvbase import svc, streams, table_io, models
from csvbase.value_objs import (
    ContentType,
    Table,
    Column,
    ColumnType,
    GitUpstream,
)
from csvbase.userdata import PGUserdataAdapter
from csvbase.follow.git import GitSource
from csvbase.config import get_config

from .conftest import ROMAN_NUMERALS
from .utils import (
    random_string,
    assert_is_valid_etag,
    create_table,
    make_user,
    current_user,
)


@pytest.fixture(
    scope="module",
    params=[
        ContentType.CSV,
        ContentType.HTML,
        ContentType.JSON,
        ContentType.JSON_LINES,
        ContentType.PARQUET,
        ContentType.XLSX,
    ],
)
def content_type(request):
    """The content type to export in."""
    # FIXME: this should probably be called "Accept"
    yield request.param


@pytest.fixture(
    params=[
        pytest.param(None, id="no upstream"),
        pytest.param("git", id="git upstream"),
    ]
)
def upstream(request):
    yield request.param


@pytest.fixture
def ten_rows(test_user, sesh, upstream, local_repos_path) -> Table:
    """Experimental version of the original fixture that varies upstream."""
    columns = [
        Column(name="roman_numeral", type_=ColumnType.TEXT),
        Column(name="is_even", type_=ColumnType.BOOLEAN),
        Column(name="as_date", type_=ColumnType.DATE),
        Column(name="as_float", type_=ColumnType.FLOAT),
    ]

    df = pd.DataFrame(
        [
            (numeral, (index % 2) == 0, date(2018, 1, index), index + 0.5)
            for index, numeral in enumerate(ROMAN_NUMERALS, start=1)
        ],
        columns=[c.name for c in columns],
    )

    table = create_table(sesh, test_user, columns, caption="Roman numerals")
    buf = StringIO()
    with streams.rewind(buf):
        df.to_csv(buf, index=0)
    dialect, columns = streams.peek_csv(buf)
    rows = table_io.csv_to_rows(buf, columns, dialect)
    backend = PGUserdataAdapter(sesh)
    backend.insert_table_data(table, columns, rows)

    if upstream == "git":
        repo_url = (
            f"https://user:pass@example.com/{random_string()}/{random_string()}.git"
        )
        local_repo_path = local_repos_path / quote_plus(repo_url)
        csv_filename = f"{random_string()}.csv"
        csv_path = local_repo_path / csv_filename
        gs = GitSource()
        gs.init_repo(local_repo_path)
        gs.initial_commit(local_repo_path)
        df.to_csv(csv_path)
        gs._run_git(["add", "."], cwd=local_repo_path)
        gs._run_git(
            ["config", "--local", "receive.denyCurrentBranch", "warn"],
            cwd=local_repo_path,
        )

        gs.commit(local_repo_path)

        last_version = gs.get_last_version(local_repo_path, csv_filename)

        git_upstream = GitUpstream(
            last_version.last_changed,
            bytes.fromhex(last_version.version_id),
            repo_url=repo_url,
            branch="main",
            path=csv_filename,
        )

        svc.create_git_upstream(sesh, table.table_uuid, git_upstream)

    sesh.commit()
    return table


def get_table(
    client,
    username: str,
    table_name: str,
    content_type: ContentType,
    extra_headers: Optional[Mapping[str, str]] = None,
) -> Response:
    """Helper function to get a table in the appropriate way given the content type."""
    headers = {}
    url = f"/{username}/{table_name}"
    if content_type in {ContentType.JSON_LINES, ContentType.PARQUET, ContentType.XLSX}:
        url += f".{content_type.file_extension()}"
    else:
        headers["Accept"] = content_type.value
    if extra_headers is not None:
        headers.update(extra_headers)
    return client.get(url, headers=headers)


def test_head__happy(client, test_user, ten_rows):
    url = f"/{test_user.username}/{ten_rows.table_name}"
    resp = client.head(url)
    assert resp.status_code == 200
    assert "ETag" not in resp.headers


def test_head__table_does_not_exist(client, test_user):
    url = f"/{test_user.username}/{random_string()}"
    resp = client.head(url)
    assert resp.status_code == 404


def test_head__user_does_not_exist(client):
    url = f"/{random_string()}/{random_string()}"
    resp = client.head(url)
    assert resp.status_code == 404


def test_head__is_private_not_authed(client, private_table, test_user):
    url = f"/{test_user.username}/{private_table}"
    resp = client.head(url)
    assert resp.status_code == 404, resp.data


def test_head__is_private_am_authed(client, private_table, test_user):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200, resp.data

    cc_obj = resp.cache_control
    assert cc_obj.private, "Cache-Control does not include 'private'"
    assert cc_obj.no_cache


def test_head__is_private_am_wrong_user(
    client, private_table, content_type, test_user, sesh, crypt_context
):
    user = make_user(sesh, crypt_context)
    sesh.commit()
    with current_user(user):
        url = f"/{test_user.username}/{private_table}"
        resp = client.head(url)
    assert resp.status_code == 404, resp.data


def test_create__happy(client, test_user, content_type):
    new_csv = """a,b,c,d,e
hello,1,1.5,FALSE,2018-01-03
"""
    table_name = random_string()
    url = f"/{test_user.username}/{table_name}"
    with current_user(test_user):
        resp = client.put(
            url,
            data=new_csv,
            headers={
                "Content-Type": "text/csv",
            },
        )
        assert resp.status_code == 201

        resp = get_table(client, test_user.username, table_name, content_type)
    assert resp.status_code == 200
    if content_type == ContentType.CSV:
        expected_df = (
            pd.read_csv(BytesIO(new_csv.encode("utf-8")))
            .assign(csvbase_row_id=[1])
            .set_index("csvbase_row_id")
        )
        actual_df = pd.read_csv(BytesIO(resp.data), index_col="csvbase_row_id")
        assert_frame_equal(expected_df, actual_df)


def test_create__doesnt_exist(client, test_user):
    new_csv = """a,b,c,d,e
hello,1,1.5,FALSE,2018-01-03
"""
    table_name = random_string()
    url = f"/{test_user.username}/{table_name}"
    with current_user(test_user):
        resp = client.put(
            url,
            data=new_csv,
            headers={"Content-Type": "text/csv", "If-None-Match": "*"},
        )
        assert resp.status_code == 201

        resp = get_table(client, test_user.username, table_name, ContentType.CSV)
    assert resp.status_code == 200


def test_create__already_exists(client, test_user, ten_rows):
    """Test that when creating you can set If-None-Match to ensure the table
    doesn't already exist.

    """
    new_csv = """csvbase_row_id,roman_numeral,is_even,as_date,as_float
,I,no,2018-01-1,1.0
    """

    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={
            "Content-Type": "text/csv",
            "Authorization": test_user.basic_auth(),
            "If-None-Match": "*",
        },
    )
    assert resp.status_code == 409


def test_create__public(client, test_user):
    new_csv = """a,b,c,d,e
hello,1,1.5,FALSE,2018-01-03
"""
    """Provide a way to set the public/private status for creation only."""
    table_name = random_string()
    url = f"/{test_user.username}/{table_name}?public=yes"
    resp = client.put(
        url,
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 201

    get_resp = get_table(client, test_user.username, table_name, ContentType.JSON)
    assert get_resp.json["is_public"], "not public"  # type: ignore


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
    assert resp.json == {
        "error": "unable to parse that csv file",
        "detail": "blank csv file",
    }


def test_create__under_another_user(sesh, client, test_user, crypt_context):
    new_csv = "a,b,c"
    table_name = random_string()
    other_user = make_user(sesh, crypt_context)
    sesh.commit()
    resp = client.put(
        f"/{test_user.username}/{table_name}",
        data=new_csv,
        headers={"Authorization": other_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 403
    assert resp.json == {"error": "that's not allowed"}


def test_create__blank_column_name(client, test_user):
    new_csv = """a,,c
1,2,3"""

    table_name = random_string()
    resp = client.put(
        f"/{test_user.username}/{table_name}?public=yes",
        data=new_csv,
        headers={"Authorization": test_user.basic_auth(), "Content-Type": "text/csv"},
    )
    assert resp.status_code == 201, resp.data

    get_resp = client.get(f"/{test_user.username}/{table_name}.parquet")
    df = pd.read_parquet(BytesIO(get_resp.data))
    assert list(df.columns) == ["csvbase_row_id", "a", "col2", "c"]


def test_create__just_header(client, test_user):
    """Users often just pass a rowless csv with just a header."""
    new_csv = "a,b,c\n"
    table_name = random_string()
    url = f"/{test_user.username}/{table_name}"
    with current_user(test_user):
        resp = client.put(
            url,
            data=new_csv,
            headers={"Content-Type": "text/csv"},
        )
        assert resp.status_code == 201, resp.data

        expected_columns = [{"name": "csvbase_row_id", "type": "integer"}]
        expected_columns.extend(
            [{"name": letter, "type": "string"} for letter in "abc"]
        )

        get_resp = client.get(url + ".json")
        assert expected_columns == get_resp.json["columns"]


def test_read__happy(client, ten_rows, test_user, content_type):
    resp = get_table(client, test_user.username, ten_rows.table_name, content_type)
    assert resp.status_code == 200, resp.data
    assert content_type.value in resp.headers["Content-Type"]

    # test that Content-Length is present, and that it matches what was
    # returned
    assert resp.headers.get("Content-Length", default=None, type=int) == len(resp.data)

    # test that the cache headers are as expected
    cc_obj = resp.cache_control
    vary = resp.headers.get("Vary")
    assert cc_obj.no_cache
    assert cc_obj.must_revalidate
    assert vary == "Accept, Cookie"

    if content_type == ContentType.HTML:
        assert cc_obj.private
    else:
        assert cc_obj.private is None

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
            "licence": None,
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


def test_read__by_accept_header(client, ten_rows, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 200
    assert resp.mimetype == content_type.value


def test_read__by_accept_header_curl(client, ten_rows, test_user):
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}", headers={"Accept": "*/*"}
    )
    assert resp.status_code == 200
    assert resp.mimetype == ContentType.CSV.value


def test_read__by_accept_header_nonsense(client, ten_rows, test_user):
    resp = client.get(
        f"/{test_user.username}/{ten_rows.table_name}",
        headers={"Accept": "application/x-shockwave-flash"},
    )
    assert resp.status_code == 406


def test_read__etag_cache_hit(client, ten_rows, test_user, content_type):
    first_resp = get_table(
        client, test_user.username, ten_rows.table_name, content_type
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    etag = first_resp.headers.get("ETag", None)

    first_cc = first_resp.cache_control
    assert first_cc.no_cache
    assert first_cc.must_revalidate
    if content_type == ContentType.HTML:
        assert etag is None
        assert first_cc.private
    else:
        assert_is_valid_etag(etag)  # type: ignore
        assert not first_cc.private

    second_resp = get_table(
        client,
        test_user.username,
        ten_rows.table_name,
        content_type,
        extra_headers={"If-None-Match": etag},  # type: ignore
    )
    second_cc = second_resp.cache_control
    assert second_cc.no_cache
    assert second_cc.must_revalidate
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
    first_resp = get_table(
        client,
        test_user.username,
        ten_rows.table_name,
        content_type,
        extra_headers={"If-None-Match": "wrong etag"},
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    etag = first_resp.headers["ETag"]

    assert_is_valid_etag(etag)


def test_read__last_changed_updates_the_etag(
    client, ten_rows, test_user, content_type, sesh
):
    if content_type == ContentType.HTML:
        pytest.skip("not relevant for html")
    first_resp = get_table(
        client, test_user.username, ten_rows.table_name, content_type
    )
    assert first_resp.status_code == 200, first_resp.data
    assert content_type.value in first_resp.headers["Content-Type"]
    first_etag = first_resp.headers["ETag"]

    svc.mark_table_changed(sesh, ten_rows.table_uuid)
    sesh.commit()

    second_resp = get_table(
        client, test_user.username, ten_rows.table_name, content_type
    )
    assert second_resp.status_code == 200, second_resp.data
    assert content_type.value in second_resp.headers["Content-Type"]
    second_etag = second_resp.headers["ETag"]

    assert first_etag != second_etag


def test_read__x_accel_redirect(client, ten_rows, test_user, content_type, sesh):
    if content_type in [ContentType.HTML, ContentType.JSON]:
        pytest.skip("not relevant for html")
    with patch.object(get_config(), "x_accel_redirect", True):
        resp = get_table(client, test_user.username, ten_rows.table_name, content_type)
    assert resp.status_code == 200
    assert resp.headers.get("X-Accel-Redirect", "").startswith("/repcache/")


def test_read__metadata_headers(client, ten_rows, test_user, content_type, sesh):
    """Check that Last-Modified and Link headers are there - these are useful to consumers.

    It's also possible that the Link header helps search engines work out that
    tables with file extensions are duplicates of the un-extensioned path.

    """
    resp = get_table(client, test_user.username, ten_rows.table_name, content_type)
    assert (
        resp.headers.get("Link")
        == f'<http://localhost/{test_user.username}/{ten_rows.table_name}>; rel="canonical"'
    )

    # HTTP's format doesn't go to the microsecond level
    # FIXME: using Last-Modified is currently disabled because it makes Varnish
    # too aggressive
    # this was added at some point to work around varnish issues, varnish is
    # gone, this is undocumented but needs to be maintained for a while
    last_mod = datetime.strptime(
        resp.headers["CSVBase-Last-Modified"], "%a, %d %b %Y %H:%M:%S GMT"
    )
    last_mod = last_mod.replace(tzinfo=timezone.utc)
    assert last_mod == ten_rows.last_changed.replace(microsecond=0)
    assert resp.last_modified == ten_rows.last_changed.replace(microsecond=0)


def test_read__with_no_rows(sesh, client, test_user, content_type):
    table = create_table(sesh, test_user, [])
    sesh.commit()
    resp = get_table(client, test_user.username, table.table_name, content_type)
    assert resp.status_code == 200


def test_read__table_does_not_exist(client, test_user, content_type):
    resp = get_table(client, test_user.username, "something", content_type)
    assert resp.status_code == 404, resp.data
    if content_type == ContentType.HTML:
        assert content_type.value in resp.headers["Content-Type"]
    else:
        assert ContentType.JSON.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that table does not exist"}


def test_read__user_does_not_exist(client, test_user, content_type):
    resp = get_table(client, random_string(), random_string(), content_type)
    assert resp.status_code == 404, resp.data
    if content_type == ContentType.HTML:
        assert content_type.value in resp.headers["Content-Type"]
    else:
        assert ContentType.JSON.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that user does not exist"}


def test_read__is_private_not_authed(client, private_table, test_user, content_type):
    resp = get_table(client, test_user.username, private_table, content_type)
    assert resp.status_code == 404, resp.data
    if content_type == ContentType.HTML:
        assert content_type.value in resp.headers["Content-Type"]
    else:
        assert ContentType.JSON.value in resp.headers["Content-Type"]
    if content_type is ContentType.JSON:
        assert resp.json == {"error": "that table does not exist"}


def test_read__is_private_am_authed(client, private_table, test_user, content_type):
    resp = client.get(
        f"/{test_user.username}/{private_table}",
        headers={"Authorization": test_user.basic_auth()},
    )
    assert resp.status_code == 200, resp.data

    cc_obj = resp.cache_control
    assert cc_obj.private, "Cache-Control does not include 'private'"
    assert cc_obj.no_cache


def test_read__is_private_am_wrong_user(
    client, private_table, content_type, test_user, sesh, crypt_context
):
    user = make_user(sesh, crypt_context)
    sesh.commit()
    with current_user(user):
        resp = get_table(client, test_user.username, private_table, content_type)
    assert resp.status_code == 404, resp.data


def test_read__empty_table(sesh, client, test_user, content_type):
    table = create_table(sesh, test_user, [])
    sesh.commit()
    resp = get_table(client, test_user.username, table.table_name, content_type)
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
    df = pd.read_csv(BytesIO(get_resp.data), index_col="csvbase_row_id")
    assert list(df.index) == [11, 12]


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
    """This follows a faster path than a normal upsert."""
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
    assert resp.json == {
        "error": "unable to parse that csv file",
        "detail": "blank csv file",
    }


def test_overwrite__with_just_header(client, test_user, ten_rows):
    new_csv = """roman_numeral,is_even,as_date,as_float
"""
    with current_user(test_user):
        resp = client.put(
            f"/{test_user.username}/{ten_rows.table_name}",
            data=new_csv,
        )
        assert resp.status_code == 200
        get_resp = client.get(
            f"/{test_user.username}/{ten_rows.table_name}.parquet",
        )
    df = pd.read_parquet(BytesIO(get_resp.data))
    assert len(df) == 0


def test_overwrite__with_just_header_including_row_id(client, test_user, ten_rows):
    new_csv = """csvbase_row_id,roman_numeral,is_even,as_date,as_float
"""
    with current_user(test_user):
        resp = client.put(
            f"/{test_user.username}/{ten_rows.table_name}",
            data=new_csv,
        )
        assert resp.status_code == 200
        get_resp = client.get(
            f"/{test_user.username}/{ten_rows.table_name}.parquet",
        )
    df = pd.read_parquet(BytesIO(get_resp.data))
    assert len(df) == 0


def test_overwrite__etag_matches(client, test_user, ten_rows):
    new_csv = """roman_numeral,is_even,as_date,as_float
"""
    url = f"/{test_user.username}/{ten_rows.table_name}"
    get_resp = client.get(url)
    resp = client.put(
        url,
        data=new_csv,
        headers={
            "Authorization": test_user.basic_auth(),
            "If-Weak-Match": get_resp.headers["ETag"],
        },
    )
    assert resp.status_code == 200


def test_overwrite__etag_doesnt_match(client, test_user, ten_rows):
    new_csv = """roman_numeral,is_even,as_date,as_float
"""
    resp = client.put(
        f"/{test_user.username}/{ten_rows.table_name}",
        data=new_csv,
        headers={
            "Authorization": test_user.basic_auth(),
            "If-Weak-Match": '"W/some junk"',
        },
    )
    assert resp.status_code == 412
    assert resp.json == {
        "error": "you provided an ETag different to the current one",
    }


@pytest.mark.parametrize("is_public", [True, False])
def test_overwrite__not_authed(sesh, client, test_user, is_public):
    table = create_table(sesh, test_user, is_public=is_public)
    sesh.commit()
    new_csv = """a
1"""
    resp = client.put(
        f"/{test_user.username}/{table.table_name}",
        data=new_csv,
    )
    if is_public:
        assert resp.status_code == 401
    else:
        assert resp.status_code == 404


@pytest.mark.parametrize("is_public", [True, False])
def test_overwrite__wrong_user(
    sesh, client, test_user, ten_rows, is_public, crypt_context
):
    user = make_user(sesh, crypt_context)
    table = create_table(sesh, user, is_public=is_public)
    sesh.commit()
    new_csv = """a
1"""
    with current_user(test_user):
        resp = client.put(
            f"/{user.username}/{table.table_name}",
            data=new_csv,
        )
    if is_public:
        assert resp.status_code == 403
    else:
        assert resp.status_code == 404


def test_overwrite__read_only(sesh, client, test_user, ten_rows, upstream):
    if upstream is None:
        pytest.skip("no upstream")
    #  mark it read-only via git repo
    git_upstream_obj = (
        sesh.query(models.GitUpstream)
        .filter(models.GitUpstream.table_uuid == ten_rows.table_uuid)
        .one()
    )
    git_upstream_obj.https_repo_url = git_upstream_obj.https_repo_url.replace(
        "user:pass", ""
    )
    sesh.commit()

    new_csv = """roman_numeral,is_even,as_date,as_float
"""
    with current_user(test_user):
        resp = client.put(
            f"/{test_user.username}/{ten_rows.table_name}",
            data=new_csv,
        )
    assert resp.status_code == 400
    assert resp.json == {"error": "that table is read-only"}


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


def test_append__just_header(client, test_user, ten_rows):
    """Users often just pass a rowless csv with just a header."""
    new_csv = "roman_numeral,is_even,as_date,as_float\n"

    with current_user(test_user):
        resp = client.post(
            f"/{test_user.username}/{ten_rows.table_name}",
            data=new_csv,
        )
        assert resp.status_code == 204

        get_resp = client.get(
            f"/{test_user.username}/{ten_rows.table_name}",
            headers={"Accept": "text/csv"},
        )
    df = pd.read_csv(BytesIO(get_resp.data))
    assert len(df) == 10


def test_append__read_only(sesh, client, test_user, ten_rows, upstream):
    if upstream is None:
        pytest.skip("no upstream")
    #  mark it read-only via git repo
    git_upstream_obj = (
        sesh.query(models.GitUpstream)
        .filter(models.GitUpstream.table_uuid == ten_rows.table_uuid)
        .one()
    )
    git_upstream_obj.https_repo_url = git_upstream_obj.https_repo_url.replace(
        "user:pass", ""
    )
    sesh.commit()

    new_csv = "roman_numeral,is_even,as_date,as_float\n"

    with current_user(test_user):
        resp = client.post(
            f"/{test_user.username}/{ten_rows.table_name}",
            data=new_csv,
        )
        assert resp.status_code == 400
        assert resp.json == {"error": "that table is read-only"}

        get_resp = client.get(
            f"/{test_user.username}/{ten_rows.table_name}",
            headers={"Accept": "text/csv"},
        )
    df = pd.read_csv(BytesIO(get_resp.data))
    assert len(df) == 10


@pytest.fixture(
    scope="module",
    params=[
        pytest.param(("{username}/{table_name}", "DELETE"), id="delete via api"),
        pytest.param(
            ("{username}/{table_name}/delete-table-form-post", "POST"),
            id="delete via form post",
        ),
    ],
)
def delete_mode(request):
    yield request.param


def test_delete__has_readme(sesh, client, test_user, ten_rows, delete_mode):
    url_template, verb = delete_mode
    svc.set_readme_markdown(sesh, ten_rows.table_uuid, "something")
    sesh.commit()
    with current_user(test_user):
        resp = client.open(
            url_template.format(
                username=test_user.username, table_name=ten_rows.table_name
            ),
            method=verb,
        )
    if verb == "POST":
        assert resp.status_code == 302
        assert resp.headers["Location"] == f"/{test_user.username}"
    else:
        assert resp.status_code == 204


@pytest.mark.parametrize("is_public", [True, False])
def test_delete__am_authed(sesh, client, test_user, is_public, ten_rows, delete_mode):
    url_template, verb = delete_mode
    with current_user(test_user):
        resp = client.open(
            url_template.format(
                username=test_user.username, table_name=ten_rows.table_name
            ),
            method=verb,
        )
    if verb == "POST":
        assert resp.status_code == 302
        assert resp.headers["Location"] == f"/{test_user.username}"
    else:
        assert resp.status_code == 204


@pytest.mark.parametrize("is_public", [True, False])
def test_delete__not_authed(sesh, client, is_public, delete_mode, test_user):
    table = create_table(sesh, test_user, is_public=is_public)
    sesh.commit()
    url_template, verb = delete_mode

    resp = client.open(
        url_template.format(username=test_user.username, table_name=table.table_name),
        method=verb,
    )
    if is_public:
        assert resp.status_code == 401
    else:
        assert resp.status_code == 404


@pytest.mark.parametrize("is_public", [True, False])
def test_delete__wrong_user(
    sesh, client, test_user, is_public, crypt_context, delete_mode
):
    table = create_table(sesh, test_user, is_public=is_public)
    sesh.commit()

    url_template, verb = delete_mode

    with current_user(make_user(sesh, crypt_context)):
        resp = client.open(
            url_template.format(
                username=test_user.username, table_name=table.table_name
            ),
            method=verb,
        )
    if is_public:
        assert resp.status_code == 403
    else:
        assert resp.status_code == 404


def test_delete__does_not_exist(client, test_user, delete_mode):
    url_template, verb = delete_mode
    with current_user(test_user):
        resp = client.open(
            url_template.format(
                username=test_user.username, table_name=random_string()
            ),
            method=verb,
        )
    assert resp.status_code == 404
