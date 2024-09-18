import json
from datetime import datetime
import itertools

from csvbase.value_objs import (
    ColumnType,
    GitUpstream,
    Licence,
    DataLicence,
)
from csvbase.conv import from_string_to_python

import pytest


def _make_versions(inp, expected):
    yield inp.lower(), expected
    yield inp.capitalize(), expected
    yield inp.upper(), expected
    yield inp[0], expected


@pytest.mark.parametrize(
    "bool_str, expected",
    itertools.chain(
        *[
            _make_versions("yes", True),
            _make_versions("no", False),
            _make_versions("true", True),
            _make_versions("false", False),
        ]
    ),
)
def test_bool_parsing_from_string(bool_str, expected):
    assert from_string_to_python(ColumnType.BOOLEAN, bool_str) == expected


def test_github_source__json_roundtrip():
    source = GitUpstream(
        last_modified=datetime(2018, 1, 3),
        last_sha=b"f" * 32,
        repo_url="https://github.com/calpaterson/csvbase.get",
        branch="main",
        path="data/moocows.csv",
    )

    assert (
        GitUpstream.from_json_dict(json.loads(json.dumps(source.to_json_dict())))
        == source
    )


@pytest.mark.parametrize(
    "repo_url, branch, expected",
    [
        (
            "https://github.com/calpaterson/csvbase.git",
            "main",
            "git+https://github.com/calpaterson/csvbase.git@main",
        ),
        pytest.param(
            "https://user:pass@github.com/calpaterson/csvbase.git",
            "main",
            "git+https://github.com/calpaterson/csvbase.git@main",
            id="auth in url",
        ),
    ],
)
def test_git_upstream__pretty_ref(repo_url, branch, expected):
    gu = GitUpstream(datetime(2018, 1, 3), b"f" * 32, repo_url, branch, "test.csv")
    assert gu.pretty_ref() == expected


@pytest.mark.parametrize(
    "repo_url, last_sha, expected",
    [
        (
            "https://github.com/calpaterson/csvbase.git",
            b"f" * 32,
            "https://github.com/calpaterson/csvbase/commit/" + (b"f" * 32).hex(),
        ),
        pytest.param(
            "https://user:pass@github.com/calpaterson/csvbase.git",
            b"f" * 32,
            "https://github.com/calpaterson/csvbase/commit/" + (b"f" * 32).hex(),
            id="auth in url",
        ),
    ],
)
def test_git_upstream__gh_commit_link(repo_url, last_sha, expected):
    gu = GitUpstream(datetime(2018, 1, 3), last_sha, repo_url, "main", "test.csv")
    assert gu.github_commit_link() == expected


@pytest.mark.parametrize(
    "repo_url, branch, path, expected",
    [
        (
            "https://github.com/calpaterson/csvbase.git",
            "wip",
            "examples/moocows.csv",
            "https://github.com/calpaterson/csvbase/blob/wip/examples/moocows.csv",
        ),
        pytest.param(
            "https://user:pass@github.com/calpaterson/csvbase.git",
            "main",
            "examples/moocows.csv",
            "https://github.com/calpaterson/csvbase/blob/main/examples/moocows.csv",
            id="auth in url",
        ),
    ],
)
def test_git_upstream__gh_link(repo_url, branch, path, expected):
    gu = GitUpstream(datetime(2018, 1, 3), b"f" * 32, repo_url, branch, path)
    assert gu.github_file_link() == expected


@pytest.mark.parametrize(
    "data_licence, expected_licence",
    [
        (DataLicence.UNKNOWN, None),
        (DataLicence.ALL_RIGHTS_RESERVED, None),
        (DataLicence.PDDL, Licence.from_spdx_id("PDDL-1.0")),
        (DataLicence.ODC_BY, Licence.from_spdx_id("ODC-By-1.0")),
        (DataLicence.ODBL, Licence.from_spdx_id("ODbL-1.0")),
        (DataLicence.OGL, Licence.from_spdx_id("OGL-UK-3.0")),
    ],
)
def test_licence_from_data_licence(data_licence, expected_licence):
    assert Licence.from_data_licence(data_licence) == expected_licence


@pytest.mark.parametrize(
    "licence, recommended",
    [
        (Licence.from_spdx_id("ODC-By-1.0"), False),
        (Licence.from_spdx_id("ODC-By-1.0"), False),
        (Licence.from_spdx_id("AGPL-3.0-or-later"), False),
        (Licence.from_spdx_id("CC0-1.0"), True),
    ],
)
def test_licence_okfn_recommended(licence, recommended):
    assert licence.recommended is recommended
