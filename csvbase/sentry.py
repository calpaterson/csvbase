from os import environ
from logging import getLogger

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

from .version import get_version

logger = getLogger(__name__)


def configure_sentry():
    environment = environ.get("CSVBASE_ENVIRONMENT", "local")
    version = get_version()
    if "CSVBASE_SENTRY_DSN" in environ:
        sentry_sdk.init(
            dsn=environ["CSVBASE_SENTRY_DSN"],
            environment=environment,
            release=version,
            in_app_include=["csvbase"],
        )
        logger.info(
            "sentry initialised (environment: '%s', release: '%s')",
            environment,
            version,
        )
    else:
        logger.info("sentry not initialised - dsn not set")
