from celery import Celery

from csvbase.config import Config

celery = Celery("csvbase.bgwork")


def initialise_celery(config: Config):
    celery.conf["broker_url"] = config.celery_broker_url

    # This retrying on startup is liable to cause confusion.  If the broker is
    # initially down, best to just crash.
    celery.conf["broker_connection_retry_on_startup"] = False
