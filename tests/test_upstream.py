from pathlib import Path

from csvbase.follow.git import GitSource

from platformdirs import user_cache_dir


def test_git_repo_path():
    git_source = GitSource()

    expected = (
        Path(user_cache_dir("csvbase"))
        / "git-repos/https___github_com_calpaterson_csvbase_git_main_8b3d4d1deb66f6eaec6fc5e6dd9ac9d00cbd4b64e5cc23850c417a21c6ce264cc786816b02dc4798554a5a57b318ef4f986396036c62b5aa1afd11d26d56ccf7"
    )
    actual = git_source.get_repo_path(
        "https://github.com/calpaterson/csvbase.git", "main"
    )
    assert expected == actual
