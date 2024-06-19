from datetime import timedelta

from urllib.parse import quote_plus

from csvbase.web.main.create_table import canonicalise_git_url, cookie_to_dict
from csvbase import exc
from csvbase.follow.git import GitSource

import pytest

from .utils import (
    parse_form,
    random_string,
    random_df,
    current_user,
)


@pytest.mark.parametrize(
    "inp, expected_output",
    [
        (
            "https://github.com/calpaterson/csvbase",
            "https://github.com/calpaterson/csvbase.git",
        ),
        (
            "git@github.com:calpaterson/csvbase.git",
            "https://github.com/calpaterson/csvbase.git",
        ),
        (
            "https://github.com/calpaterson/csvbase.git",
            "https://github.com/calpaterson/csvbase.git",
        ),
        pytest.param(
            "https://user:1234@github.com/some-user/a-repo.git",
            "https://user:1234@github.com/some-user/a-repo.git",
            id="auth token",
        ),
    ],
)
def test_canonicalise_git_url(inp, expected_output):
    assert expected_output == canonicalise_git_url(inp)


@pytest.mark.parametrize(
    "inp",
    [
        pytest.param("gasdasdasd", id="garbage"),
        pytest.param(
            "https://my-internal-git-server/some-user/a-repo.git", id="internal server"
        ),
    ],
)
def test_canonicalise_git_url__invalid(inp):
    with pytest.raises(exc.InvalidRequest):
        canonicalise_git_url(inp)


def test_get_form_blank(client, test_user):
    with current_user(test_user):
        resp = client.get("/new-table/git")
    assert resp.status_code == 200


def test_create_table__happy(client, test_user, local_repos_path):
    repo_url = f"https://github.com/calpaterson/{random_string()}"
    repo_path = local_repos_path / quote_plus(repo_url + ".git")
    gs = GitSource()
    gs.init_repo(repo_path)
    gs.initial_commit(repo_path)

    expected_df = random_df()
    with (repo_path / "test.csv").open("wb") as csv_file:
        expected_df.to_csv(csv_file, index=False)
    gs._run_git(["add", "."], cwd=repo_path)
    gs.commit(repo_path)

    # POST to create table form
    table_name = random_string()
    initial_form = {
        "table-name": table_name,
        "repo": repo_url,
        "branch": "main",
        "path": "test.csv",  # FIXME: test with and without leading slash
        "data-licence": "0",
    }
    with current_user(test_user):
        resp = client.post("/new-table/git", data=initial_form)
        assert resp.status_code == 302
        token = resp.headers["Location"].split("/")[-1]
        confirm_cookie = client.get_cookie(f"confirm-token-{token}")
        assert confirm_cookie.max_age == timedelta(hours=1).total_seconds()
        confirm_package = cookie_to_dict(
            confirm_cookie.value
        )
        assert confirm_package is not None

        confirm_get_resp = client.get(resp.headers["Location"])
    confirm_form = parse_form((confirm_get_resp.data))
    form = dict(confirm_form)

    # unique-columns is absent
    assert form == {
        "column-1-type": "TEXT",
        "column-2-type": "TEXT",
        "column-3-type": "INTEGER",
    }

    # FIXME: GET the confirm form
