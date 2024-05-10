from io import BytesIO
from logging import getLogger
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Iterable, Optional, Generator, Sequence
import contextlib
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import re

from platformdirs import user_cache_dir
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
    # In an ideal world, this would use some sort of library rather than
    # calling git as a subprocess.  However the most popular library, libgit2,
    # it not able to do some of the things done below (eg "blobless" pulls) and
    # makes others very hard.
    def repos_dir(self) -> Path:
        """Returns the ~/.cache/csvbase/git-repos dir, creating it if
        necessary.

        """
        rv = Path(user_cache_dir("csvbase")) / "git-repos"
        rv.mkdir(parents=True, exist_ok=True)
        return rv

    def run(self, command: Sequence[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a subprocess, checking that it exited happily."""
        kwargs["capture_output"] = True
        logger.info("executing '%s'", command)
        completed_process = subprocess.run(command, **kwargs)
        raise_on_error(completed_process)
        return completed_process

    def clone(self, url: str, branch: str, checkout_path: Path) -> None:
        # We are cloning "bloblessly" [actually includes many blobs] to get
        # only the most recent versions of each file.
        command = [
            "git",
            "clone",
            url,
            "-b",
            branch,
            "--filter=blob:none",
            str(checkout_path),
        ]
        self.run(command, cwd=checkout_path.parent)

    def pull(self, repo_path: Path, branch: str) -> None:
        """Pull the latest state of the repo from the remote."""
        # in order to handle rebased/changed history on the remote, this does
        # not pull but fetches and then resets to match the remote branch
        fetch_command = ["git", "fetch", "origin", branch]
        self.run(fetch_command, cwd=repo_path)

        reset_command = ["git", "reset", "--hard", f"origin/{branch}"]
        self.run(reset_command, cwd=repo_path)

    def get_last_version(self, repo_path: Path, file_path: str) -> UpstreamVersion:
        """Gets the UpstreamVersion for a specific file (that must be inside a
        repo)."""
        # Get the last version.  Possible with pygit2 but quite difficult.
        command = ["git", "log", "-n" "1", "--format=%H|%cI", file_path]
        completed_process = self.run(command, cwd=repo_path)
        raise_on_error(completed_process)
        sha, last_commit_str = (
            completed_process.stdout.decode("utf-8").strip().split("|")
        )
        last_commit_dt = isoparse(last_commit_str)
        return UpstreamVersion(last_changed=last_commit_dt, version_id=sha)

    def get_repo_path(self, url: str, branch: str) -> Path:
        """Returns the path that the repo should be stored in."""
        combined = "|".join([url, branch])
        hexdigest = hashlib.blake2b(combined.encode("utf-8")).hexdigest()
        dirname = re.sub("[^A-Za-z0-9]", "_", f"{combined}_{hexdigest}")
        return self.repos_dir() / dirname

    @contextlib.contextmanager
    def retrieve(
        self, repo_url: str, branch: str, path: str
    ) -> Generator[UpstreamFile, None, None]:
        repo_path = self.get_repo_path(repo_url, branch)
        if not repo_path.exists():
            self.clone(repo_url, branch, repo_path)
        else:
            self.pull(repo_path, branch)
        upstream_version = self.get_last_version(repo_path, path)

        # full_path: the path from cwd
        full_path = repo_path / path
        with full_path.open("rb") as retrieved_file:
            yield UpstreamFile(
                upstream_version,
                retrieved_file,
            )
