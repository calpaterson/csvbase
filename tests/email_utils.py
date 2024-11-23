import socket
import typing
from logging import getLogger
import time
from email import message_from_bytes
from email.message import EmailMessage
import email.policy
from unittest.mock import patch
import contextlib
from typing import Generator

from csvbase.email import get_smtp_host_port
from csvbase.config import get_config

from aiosmtpd.controller import Controller
from aiosmtpd.smtp import SMTP, Session, Envelope

logger = getLogger(__name__)


class StoringHandler:
    """A handler for aiosmtp which just stores emails (as stdlib EmailMessage
    objects) in a instance dict.

    """

    def __init__(self):
        self.received: dict[str, EmailMessage] = {}
        self.sleep_duration = 0.01

    async def handle_DATA(
        self, server: SMTP, session: Session, envelope: Envelope
    ) -> str:
        message = typing.cast(
            EmailMessage,
            message_from_bytes(
                envelope.original_content or b"",
                _class=EmailMessage,
                policy=email.policy.default,
            ),
        )
        # the message id needs to be stripped here, due to a bug in the stdlib
        # where whitespace is being left in front of long fields when they are
        # unwrapped
        # https://github.com/python/cpython/issues/124452
        self.received[message["Message-ID"].strip()] = message
        logger.info("Received message: '%s'", message)
        return "250 Message accepted for delivery"

    def join(self, expected: int = 1) -> None:
        """Wait until at least the expected number of emails have arrived (and been parsed)"""
        for _ in range(10):
            if len(self.received) == expected:
                logger.info("Received {self.recieved} emails")
                return None
            logger.warning(
                "Not enough email has arrived, sleeping for %f", self.sleep_duration
            )
            time.sleep(self.sleep_duration)
        else:
            raise RuntimeError("no email was delivered")


def get_free_local_port() -> int:
    """Returns a (currently) free local port.

    Not 100% effective, but very effective.  Used to prevent problems with the
    same port being used twice in tests.

    """
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

@contextlib.contextmanager
def randomise_smtp_port() -> Generator[None, None, None]:
    free_local_port = get_free_local_port()
    with patch.object(get_config(), "smtp_host", f"localhost:{free_local_port}"):
        yield


if __name__ == "__main__":
    _, port = get_smtp_host_port()
    controller = Controller(StoringHandler(), port=port)
    controller.start()
    input(f"SMTP server running on port {port}. Press Return to stop server and exit.")
    controller.stop()
