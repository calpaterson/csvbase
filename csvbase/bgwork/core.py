from celery import Celery

from csvbase.config import Config
from csvbase.web.app import init_app

celery = Celery("csvbase.bgwork")


def initialise_celery(config: Config):
    celery.conf["broker_url"] = config.celery_broker_url

    # This retrying on startup is liable to cause confusion.  If the broker is
    # initially down, best to just crash.
    celery.conf["broker_connection_retry_on_startup"] = False

    # Make sure the flask app context is pushed on all workers
    flask_app = init_app()
    flask_app.app_context().push()
