from io import BytesIO
from logging import getLogger
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Iterable, Optional, Generator
import contextlib
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from dateutil.parser import isoparse


from csvbase.value_objs import Table, GithubSource, UpstreamVersion, UpstreamFile

logger = getLogger(__name__)


def raise_on_error(completed_process: subprocess.CompletedProcess) -> None:
    """Raise an exception if the subprocess failed."""
    # FIXME: if used elsewhere this should be moved
    if completed_process.returncode != 0:
        logger.error(
            'failed to execute "%s": stderr: "%s", stdout: "%s"',
            completed_process.args,
            completed_process.stderr,
            completed_process.stdout,
        )
        raise RuntimeError(
            "git failed: %s, status code: %d",
            completed_process.args,
            completed_process.returncode,
        )
    return None


class GitSource:
    def clone(self, url: str, branch: str, checkout_path: Path):
        # We are cloning "bloblessly" [*: actually includes many blobs] to get
        # only the most recent versions of each file.
        # Not currently possible with pygit2
        command = [
            "git",
            "clone",
            url,
            "-b",
            branch,
            "--filter=blob:none",
            str(checkout_path),
        ]
        logger.info("executing '%s'", command)
        completed_process = subprocess.run(command, capture_output=True)
        raise_on_error(completed_process)

    def get_last_version(self, repo_path: Path, file_path: str) -> UpstreamVersion:
        """Gets the UpstreamVersion for a specific file (that must be inside a
        repo)."""
        # Get the last version.  Possible with pygit2 but quite difficult.
        command = ["git", "log", "--format=%H|%cI", str(file_path)]
        logger.info("executing '%s'", command)
        completed_process = subprocess.run(command, cwd=repo_path, capture_output=True)
        raise_on_error(completed_process)
        sha, last_commit_str = (
            completed_process.stdout.decode("utf-8").strip().split("|")
        )
        last_commit_dt = isoparse(last_commit_str)
        return UpstreamVersion(last_changed=last_commit_dt, version_id=sha)

    @contextlib.contextmanager
    def retrieve(
        self, repo_url: str, branch: str, path: str
    ) -> Generator[UpstreamFile, None, None]:
        # path: the path of the file to retrieve relative to the repo root
        with TemporaryDirectory() as temp_dir:
            repo_path = Path(temp_dir)
            self.clone(repo_url, branch, repo_path)
            upstream_version = self.get_last_version(repo_path, path)

            # full_path: the path from cwd
            full_path = repo_path / path
            with full_path.open("rb") as retrieved_file:
                yield UpstreamFile(
                    upstream_version,
                    retrieved_file,
                )
