from io import BytesIO
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass

from github import Github


@dataclass
class GithubFile:
    body: BytesIO
    commit_date: datetime
    sha: str


_github_obj = None


def _get_github_obj() -> Github:
    global _github_obj
    if _github_obj is None:
        _github_obj = Github()
    return _github_obj


class GithubUserdataAdapter:
    def __init__(self, backing_adapter) -> None:
        self.g = _get_github_obj()
        self.backing_adapter = backing_adapter

    def retrieve(self, org: str, repo: str, branch: str, path: str) -> GithubFile:
        repo = self.g.get_repo(f"{org}/{repo}")
        contents = repo.get_contents(path, ref=branch)
        github_file = GithubFile(
            BytesIO(contents.decoded_content),
            contents.last_modified_datetime,
            contents.sha,
        )
        return github_file
