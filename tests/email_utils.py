import typing
from logging import getLogger
import time
from email import message_from_bytes
from email.message import EmailMessage
import email.policy

from csvbase.email import get_smtp_host_port

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
                return None
            logger.warning(
                "Not enough email has arrived, sleeping for %f", self.sleep_duration
            )
            time.sleep(self.sleep_duration)
        else:
            raise RuntimeError("no email was delivered")


if __name__ == "__main__":
    _, port = get_smtp_host_port()
    controller = Controller(StoringHandler(), port=port)
    controller.start()
    input(f"SMTP server running on port {port}. Press Return to stop server and exit.")
    controller.stop()
