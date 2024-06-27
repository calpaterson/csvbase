"""This module is intended only to be run by the celery binary."""

from csvbase.config import get_config
from csvbase.web.app import init_app
from .core import initialise_celery, celery  # noqa: F401
from . import task_registry  # noqa: F401

flask_app = init_app()
initialise_celery(flask_app, get_config())
