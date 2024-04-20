from typing import Dict, Any
from os import environ
from sys import stderr
import logging

# logging module doesn't provide an easy way to get this
LOG_LEVELS = [
    "CRITICAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
]

CONFIGURED = False


def configure_logging(level: str = "INFO"):
    """Configure our logging - stderr by default but logging nicely to the
    journal under systemd."""
    global CONFIGURED
    if not CONFIGURED:
        under_systemd = "INVOCATION_ID" in environ
        kwargs: Dict[str, Any] = dict(level=level)
        if under_systemd:
            from systemd.journal import JournalHandler

            kwargs["format"] = "%(message)s"
            kwargs["handlers"] = [JournalHandler()]
        else:
            kwargs["format"] = "%(asctime)s %(levelname)-8s %(name)-35s - %(message)s"
            kwargs["stream"] = stderr
        logging.basicConfig(**kwargs)
    CONFIGURED = True
