"""A cache for generated representations of tables."""

from logging import getLogger
import tempfile
import os
from pathlib import Path
from typing import IO, Generator, Dict
from uuid import UUID
from datetime import datetime
import contextlib

from csvbase.value_objs import ContentType
from csvbase import streams

logger = getLogger(__name__)


class RepCache:
    """A cache for representations of tables.

    Reusing a previously generated representation is around 100 times faster
    than to regenerate so the speed impact of this is quite meaningful.

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
        if "w" in mode:
            with tempfile.NamedTemporaryFile(
                dir=_repcache_dir(),
                suffix=f".{content_type.file_extension()}.tmp",
                mode=mode,
            ) as temp_file:
                yield temp_file
                # to avoid corrupting the cache with partway failures, the
                # tempfile is written first and hardlinked into the final
                # position
                os.link(
                    temp_file.name, _rep_path(table_uuid, content_type, last_changed)
                )

            logger.info("wrote new representation of %s (%s)", table_uuid, content_type)
            expected_dtstr = _safe_dtstr(last_changed)
            for rep_path in _rep_dir(table_uuid).iterdir():
                if rep_path.stem != expected_dtstr:
                    rep_path.unlink()
                    logger.info(
                        "deleted old representation of %s: %s",
                        table_uuid,
                        rep_path.name,
                    )

        else:
            # it's a bit weird that we leave this open, but that is necessary
            # to stream responses at the web level
            yield _rep_path(table_uuid, content_type, last_changed).open(mode=mode)

    def exists(
        self, table_uuid: UUID, content_type: ContentType, last_changed: datetime
    ) -> bool:
        rep_path = _rep_path(table_uuid, content_type, last_changed)
        return rep_path.exists()

    def sizes(self, table_uuid: UUID, last_changed: datetime) -> Dict[ContentType, int]:
        rv = {}
        expected_dtstr = _safe_dtstr(last_changed)
        for rep_path in _rep_dir(table_uuid).iterdir():
            if rep_path.stem == expected_dtstr:
                content_type = ContentType.from_file_extension(rep_path.suffix[1:])
                if content_type is not None:
                    size = rep_path.stat().st_size
                    rv[content_type] = size
        return rv


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
