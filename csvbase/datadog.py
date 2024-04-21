from logging import getLogger
from .config import get_config

logger = getLogger(__name__)


def configure_datadog():
    config = get_config()
    if config.enable_datadog:
        logger.info("enabling datadog")
        import ddtrace.auto
    else:
        logger.info("not enabling datadog")
