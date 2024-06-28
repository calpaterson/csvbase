"""Celery beat scheduler that persists in sqlalchemy instead of a shelve file.

Experimental.

This doesn't make it any easier to edit the schedules in the database (unlike
django-celery-beat) but it does save having a file stored on disk somewhere.

Unlike the rest of csvbase, this is designed to be extracted into a separate
library at some point, so it uses only standard sqlachemy types instead of
pg-specific code.

"""

# Still to do:
# 1. Avoid timezone issues by using tz-awake SQL type for last_run_at
# 2. Make engine/table/schema configurable

from typing import Dict, Any
from logging import getLogger
import contextlib

from csvbase.db import get_db_url
from celery.beat import Scheduler, ScheduleEntry
from sqlalchemy import (
    types as satypes,
    column as sacolumn,
    table as satable,
    update,
    insert,
    delete,
    create_engine,
    func,
)
from sqlalchemy.orm import Session

logger = getLogger(__name__)

# __version__ = 1


class SQLScheduler(Scheduler):
    """Scheduler that persists schedules in SQL rather than the filesystem.

    This doesn't make schedules any easier to edit (as django-celery-beat does)
    it just avoids saving the schedule in a file on disk.
    """

    def __init__(self, *args, **kwargs):
        self._store: Dict[str, Any] = {}

        # FIXME: make this configurable
        self._sql_schema = "celery"
        self._sql_table_name = "schedule_entries"
        self._engine = create_engine(get_db_url())

        super().__init__(*args, **kwargs)

    def setup_schedule(self) -> None:
        # FIXME: We need something similar to this code (pasted from upstream) to
        # handle cases where the tz has changed under use
        # tz = self.app.conf.timezone
        # stored_tz = self._store.get("tz")
        # if stored_tz is not None and stored_tz != tz:
        #     logger.warning("Reset: Timezone changed from %r to %r", stored_tz, tz)
        #     self._store.clear()  # Timezone changed, reset db!
        # utc = self.app.conf.enable_utc
        # stored_utc = self._store.get("utc_enabled")
        # if stored_utc is not None and stored_utc != utc:
        #     choices = {True: "enabled", False: "disabled"}
        #     logger.warning(
        #         "Reset: UTC changed from %s to %s", choices[stored_utc], choices[utc]
        #     )
        #     self._store.clear()  # UTC setting changed, reset db!

        self._load_schedule()
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        # self._store.update(
        #     {
        #         "__version__": __version__,
        #         "tz": tz,
        #         "utc_enabled": utc,
        #     }
        # )
        self.sync()

    def get_schedule(self) -> Dict[str, ScheduleEntry]:
        return self._store["entries"]

    def set_schedule(self, schedule: Dict[str, ScheduleEntry]) -> None:
        self._store["entries"] = schedule

    schedule = property(get_schedule, set_schedule)

    def _get_tableclause(self):
        return satable(  # type: ignore
            self._sql_table_name,
            sacolumn("celery_app_name", type_=satypes.String),
            sacolumn("name", type_=satypes.String),
            sacolumn("created", type_=satypes.DateTime(timezone=True)),
            sacolumn("updated", type_=satypes.DateTime(timezone=True)),
            sacolumn("pickled_schedule_entry", type_=satypes.PickleType()),
            schema=self._sql_schema,
        )

    def _load_schedule(self) -> None:
        """Load the schedule into memory from the SQL database."""
        table = self._get_tableclause()
        entries: Dict[str, ScheduleEntry] = {}
        with contextlib.closing(Session(self._engine)) as session:
            logger.info("loading schedule from SQL database")
            entry_rows = session.query(  # type: ignore
                table.c.name, table.c.pickled_schedule_entry
            ).where(table.c.celery_app_name == self.app.main)
            for name, schedule_entry in entry_rows:
                logger.info("found: %s", schedule_entry)
                entries[name] = schedule_entry
        self._store["entries"] = entries

    def sync(self) -> None:
        table = self._get_tableclause()
        schedule = self._store["entries"]
        names = set(self._store["entries"].keys())

        with contextlib.closing(Session(self._engine)) as session:
            names_in_db = set(
                t[0]
                for t in session.query(table.c.name).where(  # type: ignore
                    table.c.celery_app_name == self.app.main
                )
            )

            removed_names = names_in_db.difference(names)
            if len(removed_names) > 0:
                session.execute(
                    delete(table).where(  # type: ignore
                        table.c.name.in_(removed_names),
                        table.c.celery_app_name == self.app.main,
                    )
                )
                logger.info("removed: %s", removed_names)

            new_names = names.difference(names_in_db)
            if len(new_names) > 0:
                session.execute(
                    insert(table).values(
                        [
                            {
                                "celery_app_name": self.app.main,
                                "name": new_name,
                                "pickled_schedule_entry": schedule[new_name],
                                "updated": func.now(),
                                "created": func.now(),
                            }
                            for new_name in new_names
                        ]
                    )
                )
                logger.info("added: %s", new_names)

            possibly_changed_names = names.intersection(names_in_db)
            changed_names = []
            for possibly_changed_name in possibly_changed_names:
                schedule_entry = schedule[possibly_changed_name]
                stmt = (
                    update(table)  # type: ignore
                    .where(
                        table.c.name == possibly_changed_name,
                        table.c.celery_app_name == self.app.main,
                        table.c.pickled_schedule_entry != schedule_entry,
                    )
                    .values(
                        {
                            "pickled_schedule_entry": schedule_entry,
                            "updated": func.now(),
                        }
                    )
                )
                rv = session.execute(stmt)
                if rv.rowcount != 0:
                    changed_names.append(possibly_changed_name)
            if len(changed_names) > 0:
                logger.info("updated: %s", changed_names)
            session.commit()

    def close(self) -> None:
        self.sync()

    def _get_schedule_url(self) -> str:
        return self._engine.url

    @property
    def info(self) -> str:
        return f"    . db -> {self._get_schedule_url()}"
