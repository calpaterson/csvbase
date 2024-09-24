import typing
from logging import getLogger
import time
from email import message_from_bytes
from email.message import EmailMessage
import email.policy

from csvbase.email import get_smtp_host_port

from aiosmtpd.controller import Controller

logger = getLogger(__name__)


class StoringHandler:
    def __init__(self):
        self.received: dict[str, EmailMessage] = {}
        self.sleep_duration = 0.01

    async def handle_DATA(self, server, session, envelope) -> str:
        message = typing.cast(
            EmailMessage,
            message_from_bytes(
                envelope.original_content,
                _class=EmailMessage,
                policy=email.policy.default,
            ),
        )
        # the message id needs to be stripped here, I think because of a bug in
        # the stdlib where whitespace is being put in front long fields when
        # they are wrapped
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
