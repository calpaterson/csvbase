from logging import getLogger
from typing import Optional, Generator
from smtplib import SMTP
from email.message import EmailMessage
import urllib.parse
import contextlib

from csvbase.config import get_config

logger = getLogger(__name__)

# A short timeout because this is designed to send to a local MTA
SMTP_TIMEOUT = 1


def get_smtp_host_port() -> tuple[str, int]:
    parsed = urllib.parse.urlparse(f"//{get_config().smtp_host}")
    return parsed.hostname, parsed.port  # type: ignore


@contextlib.contextmanager
def make_smtp_sesh() -> Generator[SMTP, None, None]:
    host, port = get_smtp_host_port()
    try:
        smtp_sesh = SMTP(host, port, timeout=SMTP_TIMEOUT)
        logger.debug("SMTP connection created to %s:%d", host, port)
        yield smtp_sesh
    finally:
        smtp_sesh.quit()
        logger.debug("Closed SMTP connection with %s:%d", host, port)


def email_is_enabled() -> bool:
    return get_config().smtp_host is not None


def send(message: EmailMessage, smtp_sesh: Optional[SMTP] = None) -> None:
    """Send an email."""
    if "message-id" not in message:
        raise RuntimeError("Must set a message id")
    if email_is_enabled():
        if smtp_sesh is None:
            with make_smtp_sesh() as smtp_sesh:
                smtp_sesh.send_message(message)
        else:
            smtp_sesh.send_message(message)
        logger.info("Sent email: %s", message["message-id"])


class Outbox:
    """A "transactional outbox" that allows queuing up email to send and then
    sending them all at once at the end.

    """

    def __init__(self, smtp_sesh: SMTP) -> None:
        self.smtp_sesh = smtp_sesh
        self.stack: list[EmailMessage] = []

    def enqueue(self, message: EmailMessage) -> None:
        if "message-id" not in message:
            raise RuntimeError("Must set a message id")
        self.stack.append(message)

    def flush(self) -> None:
        while len(self.stack) > 0:
            message = self.stack.pop(0)
            send(message, self.smtp_sesh)
