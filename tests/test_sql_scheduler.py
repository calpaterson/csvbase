from datetime import datetime, timezone, timedelta

import pytest
from celery import Celery
from celery.beat import ScheduleEntry

from csvbase.db import get_db_url
from csvbase.bgwork.sql_scheduler import SQLAlchemyScheduler

from .utils import random_string


@pytest.fixture(scope="function")
def celery_app():
    celery = Celery(random_string())
    celery.conf["beat_sqlalchemy_scheduler_db_url"] = get_db_url()
    celery.conf["beat_scheduler"] = "csvbase.bgwork.sql_scheduler:SQLScheduler"
    return celery


def test_sql_schedule__get_initial_schedule(celery_app):
    scheduler = SQLAlchemyScheduler(celery_app)

    schedule = scheduler.get_schedule()
    assert set(schedule.keys()) == {"celery.backend_cleanup"}


def make_schedule_entry(celery_app) -> ScheduleEntry:
    name = f"{random_string()}()"
    last_run_at = datetime.now(timezone.utc)
    options = {"expires": 100}
    return ScheduleEntry(
        app=celery_app,
        name=name,
        task=name,
        args=(),
        kwargs={},
        options=options,
        schedule=None,  # FIXME: this should be real
        last_run_at=last_run_at,
        total_run_count=0,
    )


def test_sql_schedule__test_persistence_works(celery_app):
    @celery_app.task
    def example_task():
        return

    celery_app.add_periodic_task(
        timedelta(minutes=30).total_seconds(), example_task.s()
    )

    scheduler = SQLAlchemyScheduler(celery_app)
    schedule = scheduler.get_schedule()
    assert set(schedule.keys()) == {
        "celery.backend_cleanup",
        "tests.test_sql_scheduler.example_task()",
    }
