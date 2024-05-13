"""A way to store uploads for short periods of time between pages, with auto-cleanup.

This currently writes to the filesystem and so prevents scaling across multiple
machines.  The API encapsulates that, and it should be an easy job later to
adjust to that it writes to an object store.

"""

from typing import IO, Generator
from logging import getLogger
from pathlib import Path
import secrets
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
import shutil
import gzip

from platformdirs import user_cache_dir

from . import exc
from .streams import rewind

logger = getLogger(__name__)

DEFAULT_RETENTION = timedelta(hours=1)

_logged = False


def get_temp_dir() -> Path:
    global _logged
    # using /tmp would mean we lose the cache between restarts
    temp_dir = Path(user_cache_dir("csvbase"))
    if not _logged:
        logger.info("temp dir: %s", temp_dir)
        _logged = True
    if not temp_dir.exists():
        logger.info("creating temp dir")
        temp_dir.mkdir()
    return temp_dir


def store_temp_file(
    filelike: IO[bytes], duration: timedelta = DEFAULT_RETENTION
) -> str:
    cleanup_temp_files()
    file_id = secrets.token_urlsafe()
    expiry = datetime.now(timezone.utc) + duration
    # probably no reason to care about ntfs here, but colons make it go bonkers
    # so omit them
    expiry_str = expiry.isoformat().replace(":", "_")
    filename = f"expires{expiry_str}__{file_id}.gz"
    with rewind(filelike):
        with gzip.GzipFile(get_temp_dir() / filename, mode="wb") as temp_file:
            shutil.copyfileobj(filelike, temp_file)
    return file_id


@contextmanager
def retrieve_temp_file(file_id: str) -> Generator[gzip.GzipFile, None, None]:
    cleanup_temp_files()
    temp_dir = get_temp_dir()
    globbed = list(temp_dir.glob(f"*__{file_id}.gz"))
    if len(globbed) != 1:
        raise exc.MissingTempFile()
    else:
        filename = globbed[0]
        with gzip.GzipFile(filename, mode="rb") as filelike:
            yield filelike


def cleanup_temp_files() -> None:
    temp_dir = get_temp_dir()
    delete_count = 0
    left_count = 0
    now = datetime.now(timezone.utc)
    for e in temp_dir.glob("expires*__*.gz"):
        expiry_str = e.name.split("__")[0][len("expires") :].replace("_", ":")
        expiry = datetime.fromisoformat(expiry_str)
        if expiry < now:
            e.unlink()
            delete_count += 1
        else:
            left_count += 1
