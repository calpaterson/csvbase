"""A cache for generated representations of tables."""

from logging import getLogger
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

    def __init__(
        self, table_uuid: UUID, content_type: ContentType, last_changed: datetime
    ) -> None:
        self.table_uuid = table_uuid
        self.content_type = content_type
        self.last_changed = last_changed

    def write_in_progress(self) -> bool:
        """Returns true if this rep is currently being written."""
        return self._temp_path().exists()

    @contextlib.contextmanager
    def open(
        self,
        mode: str = "rb",
    ) -> Generator[IO[bytes], None, None]:
        if "w" in mode:
            try:
                with self._temp_path().open(mode) as temp_file:
                    # make sure the file exists on disk:
                    temp_file.flush()
                    yield temp_file
                    # to avoid corrupting the cache with partway failures, the
                    # tempfile is written first and hardlinked into the final
                    # position
                    os.link(
                        temp_file.name,
                        self._rep_path(),
                    )
            finally:
                # try to ensure that the temp file is unlinked in the event of a crash
                self._temp_path().unlink(missing_ok=True)

            logger.info(
                "wrote new representation of %s (%s)",
                self.table_uuid,
                self.content_type,
            )
            expected_dtstr = _safe_dtstr(self.last_changed)
            for rep_path in _rep_dir(self.table_uuid).iterdir():
                if rep_path.stem != expected_dtstr:
                    rep_path.unlink()
                    logger.info(
                        "deleted old representation of %s: %s",
                        self.table_uuid,
                        rep_path.name,
                    )

        else:
            # it's a bit weird that we leave this open, but that is necessary
            # to stream responses at the web level
            yield self._rep_path().open(mode=mode)

    def exists(self) -> bool:
        rep_path = self._rep_path()
        return rep_path.exists()

    @staticmethod
    def sizes(table_uuid: UUID, last_changed: datetime) -> Dict[ContentType, int]:
        """Return the sizes of the various representations held."""

        rv = {}
        expected_dtstr = _safe_dtstr(last_changed)
        for rep_path in _rep_dir(table_uuid).iterdir():
            if rep_path.stem == expected_dtstr:
                content_type = ContentType.from_file_extension(rep_path.suffix[1:])
                if content_type is not None:
                    size = rep_path.stat().st_size
                    rv[content_type] = size
        return rv

    def path(self) -> str:
        """Returns the path of a specific representation's file on disk,
        relative to the repcache root directory.

        This is used for X-Accel-Redirect.

        """
        rep_path = self._rep_path()
        return str(rep_path.relative_to(_repcache_dir()))

    def _rep_path(self) -> Path:
        safe_dtstr = _safe_dtstr(self.last_changed)
        rep_dir = _rep_dir(self.table_uuid)
        return rep_dir / f"{safe_dtstr}.{self.content_type.file_extension()}"

    def _temp_path(self) -> Path:
        safe_dtstr = _safe_dtstr(self.last_changed)
        rep_dir = _rep_dir(self.table_uuid)
        return rep_dir / f"{safe_dtstr}.{self.content_type.file_extension()}.tmp"


def _safe_dtstr(dt: datetime) -> str:
    # cut out colons, which cause problems on ntfs
    return dt.isoformat().replace(":", "_")


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
