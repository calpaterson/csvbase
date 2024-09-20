from logging import getLogger
import time
from email import message_from_bytes
import email.policy

from csvbase.email import get_smtp_host_port

from aiosmtpd.controller import Controller

logger = getLogger(__name__)


class StoringHandler:
    def __init__(self):
        self.received = []
        self.sleep_duration = 0.01

    async def handle_DATA(self, server, session, envelope) -> str:
        message = message_from_bytes(
            envelope.original_content, policy=email.policy.default
        )
        self.received.append(message)
        logger.info("Received message: '%s'", message)
        return "250 Message accepted for delivery"

    def join(self) -> None:
        """Wait until at least one email has arrived (and been parsed)"""
        for _ in range(10):
            if len(self.received) > 0:
                return None
            logger.warning("No email has arrived, sleeping for %f", self.sleep_duration)
            time.sleep(self.sleep_duration)
        else:
            raise RuntimeError("no email was delivered")


if __name__ == "__main__":
    _, port = get_smtp_host_port()
    controller = Controller(StoringHandler(), port=port)
    controller.start()
    input(f"SMTP server running on port {port}. Press Return to stop server and exit.")
    controller.stop()
