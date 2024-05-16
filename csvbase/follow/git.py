from logging import getLogger
from typing import Generator, Sequence
import contextlib
import subprocess
from pathlib import Path
import hashlib
import re

from platformdirs import user_cache_dir
from dateutil.parser import isoparse


from csvbase.value_objs import UpstreamVersion, UpstreamFile

logger = getLogger(__name__)


def raise_on_error(completed_process: subprocess.CompletedProcess) -> None:
    """Raise an exception if the subprocess failed."""
    if completed_process.returncode != 0:
        logger.error(
            'failed to execute "%s": stderr: "%s", stdout: "%s"',
            completed_process.args,
            completed_process.stderr,
            completed_process.stdout,
        )
        # FIXME: this should raise some kind of git-specific error
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

    def run_git(self, git_args: Sequence[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a subprocess, checking that it exited happily."""
        kwargs["capture_output"] = True
        command = ["git"]
        command.extend(git_args)
        logger.info("executing '%s'", command)
        completed_process = subprocess.run(command, **kwargs)
        raise_on_error(completed_process)
        return completed_process

    def clone(self, url: str, branch: str, checkout_path: Path) -> None:
        # We are cloning "bloblessly" [actually includes many blobs] to get
        # only the most recent versions of each file.
        git_args = [
            "clone",
            url,
            "-b",
            branch,
            "--filter=blob:none",
            str(checkout_path),
        ]
        self.run_git(git_args, cwd=checkout_path.parent)
        self.set_identity(checkout_path)

    def init_repo(self, repo_path: Path) -> None:
        """Create a new repo on the given path.

        For compatibility with Github, the initial branch is 'main'.

        """
        self.run_git(
            ["init", "--initial-branch=main", str(repo_path)], cwd=repo_path.parent
        )
        self.set_identity(repo_path)

    def initial_commit(self, repo_path: Path) -> None:
        """Create the standard 'Initial commit'.

        Mainly useful for testing at this point.

        """
        self.run_git(["commit", "--allow-empty", "-m", "Initial commit"], cwd=repo_path)

    def commit(self, repo_path: Path, message: str = "csvbase commit") -> None:
        """Commit the current state of the repo.

        Note that untracked files must be "git add"'d at least once prior to a
        commit.

        """
        self.run_git(["commit", "-a", "-m", message], cwd=repo_path)

    def set_identity(self, repo_path: Path) -> None:
        """Set the git author and author email."""
        self.run_git(["config", "--local", "user.name", "csvbase"], cwd=repo_path)
        # FIXME: this email address should be configurable
        self.run_git(
            ["config", "--local", "user.email", "git@csvbase.com"], cwd=repo_path
        )

    def pull(self, repo_path: Path, branch: str) -> None:
        """Pull the latest state of the repo from the remote."""
        # in order to handle rebased/changed history on the remote, this does
        # not pull but fetches and then resets to match the remote branch
        fetch_git_args = ["fetch", "origin", branch]
        self.run_git(fetch_git_args, cwd=repo_path)

        reset_git_args = ["reset", "--hard", f"origin/{branch}"]
        self.run_git(reset_git_args, cwd=repo_path)

    def get_last_version(self, repo_path: Path, file_path: str) -> UpstreamVersion:
        """Gets the UpstreamVersion for a specific file (that must be inside a
        repo)."""
        # Get the last version.  Possible with pygit2 but quite difficult.
        git_args = ["log", "-n1", "--format=%H|%cI", file_path]
        completed_process = self.run_git(git_args, cwd=repo_path)
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
