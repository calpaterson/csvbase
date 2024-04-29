from io import BytesIO
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Iterable

from csvbase.value_objs import Table, GithubSource

from github import Github


@dataclass
class GithubFile:
    body: BytesIO
    commit_date: datetime
    sha: bytes


_github_obj = None


def _get_github_obj() -> Github:
    global _github_obj
    if _github_obj is None:
        _github_obj = Github()
    return _github_obj


class GithubFollower:
    def __init__(self) -> None:
        self.g = _get_github_obj()

    def retrieve(self, org: str, repo: str, branch: str, path: str) -> GithubFile:
        # FIXME: last_sha does not appear to be the right thing.  we want last
        # commit hash.  more work required.
        repo_obj = self.g.get_repo(f"{org}/{repo}")
        contents = repo_obj.get_contents(path, ref=branch)
        # FIXME: This is incredibly slow for large files and needs some kind of alternative
        github_file = GithubFile(
            BytesIO(contents.decoded_content),  # type: ignore
            contents.last_modified_datetime,  # type: ignore
            bytes.fromhex(contents.sha),  # type: ignore
        )
        return github_file

    # def tables(self) -> Iterable[Table]:
    #     ...
