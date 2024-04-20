from unittest.mock import ANY

from csvbase.web.func import set_current_user
from csvbase.web.main.create_table import parse_github_url, cookie_to_dict

import pytest

from .utils import parse_form, random_string


@pytest.mark.parametrize(
    "inp, expected_output",
    [("https://github.com/calpaterson/csvbase", ("calpaterson", "csvbase"))],
)
def test_parse_github_url(inp, expected_output):
    assert expected_output == parse_github_url(inp)


def test_get_form_blank(client, test_user):
    set_current_user(test_user)
    resp = client.get("/new-table/git")
    assert resp.status_code == 200


@pytest.mark.skip(reason="this test does uncontrolled IO to github")
def test_create_table__happy(client, test_user):
    set_current_user(test_user)
    table_name = random_string()
    initial_form = {
        "table-name": table_name,
        "repo": "https://csvbase.com/calpaterson/csvbase",
        "branch": "main",
        "path": "/examples/moocows.csv",
    }
    resp = client.post("/new-table/git", data=initial_form)
    assert resp.status_code == 302
    token = resp.headers["Location"].split("/")[-1]
    confirm_package = cookie_to_dict(client.get_cookie(f"confirm-token-{token}").value)
    assert confirm_package is not None

    confirm_get_resp = client.get(resp.headers["Location"])
    form = dict(parse_form(confirm_get_resp.data))
    assert form == {
        "column-1-type": "TEXT",
        "column-2-type": "INTEGER",
        "column-1-is-unique": "",
        "column-2-is-unique": "",
    }