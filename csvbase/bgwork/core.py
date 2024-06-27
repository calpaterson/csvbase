from celery import Celery
from flask import Flask

from csvbase.config import Config
from csvbase.web.app import init_app

celery = Celery("csvbase.bgwork")


def initialise_celery(flask_app: Flask, config: Config) -> None:
    celery.conf["broker_url"] = config.celery_broker_url

    # This retrying on startup is liable to cause confusion.  If the broker is
    # initially down, best to just crash.
    celery.conf["broker_connection_retry_on_startup"] = False

    # Make sure the flask app context is pushed for all tasks.  A few type
    # ignores here.  Based on: https://stackoverflow.com/a/50665633
    TaskBase = celery.Task
    class FlaskContextTask(TaskBase):  # type: ignore
        abstract = True
        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = FlaskContextTask  # type: ignore
