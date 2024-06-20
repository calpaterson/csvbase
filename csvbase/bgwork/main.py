"""This module is intended only to be run by the celery binary."""

from csvbase.config import get_config
from .core import initialise_celery, celery  # noqa: F401
from . import task_registry  # noqa: F401

initialise_celery(get_config())
