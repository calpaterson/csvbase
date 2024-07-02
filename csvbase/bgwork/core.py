from logging import getLogger

from celery import Celery, Task
from flask import Flask

from csvbase.config import Config
from csvbase.db import get_db_url

logger = getLogger(__name__)

celery = Celery("csvbase.bgwork")


def initialise_celery(flask_app: Flask, config: Config) -> None:
    celery.conf["broker_url"] = config.celery_broker_url
    celery.conf["worker_hijack_root_logger"] = False

    # This retrying on startup is liable to cause confusion.  If the broker is
    # initially down, best to just crash.
    celery.conf["broker_connection_retry_on_startup"] = False

    celery.conf["beat_scheduler"] = "csvbase.bgwork.sql_scheduler:SQLAlchemyScheduler"
    celery.conf["beat_sqlalchemy_scheduler_db_url"] = get_db_url()

    # Make sure the flask app context is pushed for all tasks.
    class FlaskContextTask(Task):
        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return self.run(*args, **kwargs)

    celery.task_cls = FlaskContextTask  # type: ignore
    celery.set_default()
    logger.info("initialised celery")
