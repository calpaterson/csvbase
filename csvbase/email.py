from logging import getLogger
from typing import Optional
from smtplib import SMTP
from email.message import EmailMessage
import urllib.parse

from csvbase.config import get_config

logger = getLogger(__name__)

_smtp_sesh: Optional[SMTP] = None

# A short timeout because this is designed to send to a local MTA
SMTP_TIMEOUT = 1


def get_smtp_host_port() -> tuple[str, int]:
    parsed = urllib.parse.urlparse(f"//{get_config().smtp_host}")
    return parsed.hostname, parsed.port


def get_smtp_sesh() -> SMTP:
    global _smtp_sesh
    if _smtp_sesh is None:
        host, port = get_smtp_host_port()
        _smtp_sesh = SMTP(host, port, timeout=SMTP_TIMEOUT)
        logger.info("SMTP connection created to %s:%d", host, port)
    return _smtp_sesh


def email_is_enabled() -> bool:
    return get_config().smtp_host is not None


def send(message: EmailMessage) -> None:
    """Send an email."""
    if email_is_enabled():
        smtp_sesh = get_smtp_sesh()
        smtp_sesh.send_message(message)
