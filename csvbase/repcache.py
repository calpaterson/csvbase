"""A cache for generated representations of tables."""

from pathlib import Path
from typing import IO, Generator
from uuid import UUID
from datetime import datetime
import contextlib

from csvbase.value_objs import ContentType
from csvbase import streams


class RepCache:
    """A cache for representations of tables.

    This is currently implemented with files, but that is completely
    encapsulated so it should be easier to port to S3 later on.

    """
    @contextlib.contextmanager
    def open(
        self,
        table_uuid: UUID,
        content_type: ContentType,
        last_changed: datetime,
        mode: str = "rb",
    ) -> Generator[IO[bytes], None, None]:
        with _rep_path(table_uuid, content_type, last_changed).open(
            mode=mode
        ) as file_obj:
            yield file_obj

        # cleanup old reps if we wrote
        if "w" in mode:
            expected_dtstr = _safe_dtstr(last_changed)
            for rep_path in _rep_dir(table_uuid).iterdir():
                if rep_path.stem != expected_dtstr:
                    rep_path.unlink()

    def exists(
        self, table_uuid: UUID, content_type: ContentType, last_changed: datetime
    ) -> bool:
        rep_path = _rep_path(table_uuid, content_type, last_changed)
        return rep_path.exists()


def _safe_dtstr(dt: datetime) -> str:
    # cut out colons, which cause problems on ntfs
    return dt.isoformat().replace(":", "_")


def _rep_path(
    table_uuid: UUID, content_type: ContentType, last_changed: datetime
) -> Path:
    safe_dtstr = _safe_dtstr(last_changed)
    rep_dir = _rep_dir(table_uuid)
    return rep_dir / f"{safe_dtstr}.{content_type.file_extension()}"


def _rep_dir(table_uuid: UUID) -> Path:
    rep_dir = _repcache_dir() / str(table_uuid)
    if not rep_dir.exists():
        rep_dir.mkdir()
    return rep_dir


_REPCACHE_DIR_EXISTS = False


def _repcache_dir() -> Path:
    """Returns the "stage dir" which is used for keeping generated files."""
    global _REPCACHE_DIR_EXISTS
    stage_dir = streams.cache_dir() / "repcache"
    if not _REPCACHE_DIR_EXISTS:
        stage_dir.mkdir(exist_ok=True)
        _REPCACHE_DIR_EXISTS = True
    return stage_dir
