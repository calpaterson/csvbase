from pathlib import Path

from platformdirs import user_cache_dir

from csvbase.follow.git import get_repo_path

import pytest


@pytest.mark.parametrize(
    "url, branch, expected_dirname",
    [
        (
            "https://github.com/calpaterson/csvbase.git",
            "main",
            "github_com_calpaterson_csvbase_git_main_8b3d4d1deb66f6eaec6fc5e6dd9ac9d00cbd4b64e5cc23850c417a21c6ce264cc786816b02dc4798554a5a57b318ef4f986396036c62b5aa1afd11d26d56ccf7",
        ),
        pytest.param(
            "https://github.com/calpaterson/" + ("f" * 2000),
            "main",
            "github_com_calpaterson_ffffffffffffffffffffffffffffffffffffffffffffffff_265f962f2f89654153b90113677147e870576168fd59bc598ebafdabf9fb5c9297f3b1895798d5866b1e05c442c940c6e85e9ced102734637080ab2d531b4e5b",
            id="huge url",
        ),
    ],
)
def test_git_repo_path(url, branch, expected_dirname):
    expected = Path(user_cache_dir("csvbase")) / "git-repos" / expected_dirname
    actual = get_repo_path(url, branch)
    assert expected == actual
    assert len(actual.name) <= 200
