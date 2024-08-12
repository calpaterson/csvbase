"""Celery beat scheduler that persists in SQL instead of a shelve file.

Unlike the rest of csvbase, this uses only standard sqlalchemy types as it is
designed to be extracted into a separate library at some point.

"""

from typing import Dict
from logging import getLogger
import contextlib

from celery import Celery
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
from sqlalchemy import URL
from sqlalchemy.sql.expression import TableClause
from sqlalchemy.orm import Session

logger = getLogger(__name__)


class SQLAlchemyScheduler(Scheduler):
    """Scheduler that persists schedules in SQL (via SQLAlchemy) rather than
    the filesystem.

    This doesn't make schedules any easier to edit (as django-celery-beat does)
    it just avoids saving the schedule in a file on disk.
    """

    def __init__(self, app: Celery, *args, **kwargs):
        self._entries: Dict[str, ScheduleEntry] = {}

        db_url = app.conf.get("beat_sqlalchemy_scheduler_db_url")
        if db_url is None:
            raise RuntimeError(
                "you must set the celery conf variable 'beat_sqlalchemy_scheduler_db_url'"
            )
        self._engine = create_engine(db_url)
        self._sql_schema: str = app.conf.get(
            "beat_sqlalchemy_scheduler_schema", default="celery"
        )
        self._sql_table_name: str = app.conf.get(
            "beat_sqlalchemy_scheduler_table_name", default="schedule_entries"
        )

        super().__init__(app, *args, **kwargs)

    def setup_schedule(self) -> None:
        """Called in the superclass to initialise the scheduler (when lazy=False)."""
        # There seem to be numerous long standing bugs in this area.
        # https://github.com/celery/celery/issues/4842
        # https://github.com/celery/celery/issues/2649
        # https://github.com/celery/celery/issues/4006
        # Because the ScheduleEntry objects are pickled (as in the official
        # PersistentScheduler class) we don't have the ability to correct them
        # for changes to timezone configuration
        if self.app.conf.timezone not in [None, "UTC"] or not self.app.conf.enable_utc:
            logger.warning(
                "You have changed 'timezone' or 'enable_utc' from the default.  Beware issues with periodic tasks firing at the wrong times."
            )
        self._load_schedule()
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        self.sync()

    def get_schedule(self) -> Dict[str, ScheduleEntry]:
        """Return the schedule."""
        return self._entries

    def set_schedule(self, schedule: Dict[str, ScheduleEntry]) -> None:
        """Set the schedule.  This does not send the schedule to the database
        as that is done manually via .sync()."""
        self._entries = schedule

    schedule = property(get_schedule, set_schedule)

    def _get_tableclause(self) -> TableClause:
        """Returns a SQLAlchemy TableClause for the table the schedule is kept in."""
        return satable(
            self._sql_table_name,
            sacolumn("celery_app_name", type_=satypes.String),
            sacolumn("name", type_=satypes.String),
            sacolumn("created", type_=satypes.DateTime(timezone=True)),
            sacolumn("updated", type_=satypes.DateTime(timezone=True)),
            sacolumn("pickled_schedule_entry", type_=satypes.PickleType()),
            schema=self._sql_schema,
        )

    def _load_schedule(self) -> None:
        """Load the schedule into memory from the SQL database.  Only used once, at startup."""
        table = self._get_tableclause()
        entries: Dict[str, ScheduleEntry] = {}
        with contextlib.closing(Session(self._engine)) as session:
            entry_rows = session.query(
                table.c.name, table.c.pickled_schedule_entry
            ).where(table.c.celery_app_name == self.app.main)
            for name, schedule_entry in entry_rows:
                logger.info("found schedule entry: %s", schedule_entry)
                entries[name] = schedule_entry
        self._entries = entries
        logger.info("loaded schedule from SQL database")

    def sync(self) -> None:
        """Write out (transactionally) the schedule to the database."""
        table = self._get_tableclause()
        schedule = self._entries
        names = set(self._entries.keys())

        with contextlib.closing(Session(self._engine)) as session:
            names_in_db = set(
                t[0]
                for t in session.query(table.c.name).where(
                    table.c.celery_app_name == self.app.main
                )
            )

            removed_names = names_in_db.difference(names)
            if len(removed_names) > 0:
                session.execute(
                    delete(table).where(
                        table.c.name.in_(removed_names),
                        table.c.celery_app_name == self.app.main,
                    )
                )
                logger.info("removed schedule entry %s", removed_names)

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
                logger.info("added schedule entry: %s", new_names)

            possibly_changed_names = names.intersection(names_in_db)
            changed_names = []
            for possibly_changed_name in possibly_changed_names:
                schedule_entry = schedule[possibly_changed_name]
                stmt = (
                    update(table)
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
                logger.info("updated schedule entry: %s", changed_names)
            session.commit()
            logger.info("synced schedule to SQL database")

    def close(self) -> None:
        # Nothing to close so just sync
        self.sync()

    def _get_schedule_url(self) -> URL:
        return self._engine.url

    @property
    def info(self) -> str:
        return f"    . db -> {self._get_schedule_url()}"
