from logging import getLogger

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

from .config import get_config
from .value_objs import User
from .version import get_version

logger = getLogger(__name__)


def configure_sentry():
    config = get_config()
    version = get_version()
    if config.sentry_dsn is not None:
        sentry_sdk.init(
            dsn=config.sentry_dsn,
            environment=config.environment,
            release=version,
            in_app_include=["csvbase"],
            integrations=[FlaskIntegration()],
        )
        logger.info(
            "sentry initialised (environment: '%s', release: '%s')",
            config.environment,
            version,
        )
    else:
        logger.info("sentry not initialised - dsn not set")


def set_user(user: User) -> None:
    """Set the user in sentry.

    This allows knowing how many people were affected by a bug."""
    config = get_config()
    if config.sentry_dsn is not None:
        # don't set username/email etc for privacy reasons
        sentry_sdk.set_user({"id": str(user.user_uuid)})
