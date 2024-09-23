from email.message import EmailMessage

import pytest

from csvbase import email

from . import utils


def make_message() -> EmailMessage:
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "receiver@example.come"
    message["Subject"] = "Test"
    message.set_content("Test")

    return message


def test_email__send_email(mock_smtpd):
    message_id = f"<test-{utils.random_string()}@csvbase.com>"
    message = make_message()
    message.add_header("Message-ID", message_id)

    email.send(message)
    mock_smtpd.join()

    assert message_id in mock_smtpd.received


def test_email__send_email_without_message_id(mock_smtpd):
    message = make_message()
    with pytest.raises(RuntimeError):
        email.send(message)


def test_email__outbox(mock_smtpd):
    message_ids = []
    with email.make_smtp_sesh() as smtp_sesh:
        outbox = email.Outbox(smtp_sesh)
        for _ in range(5):
            message = make_message()
            message_id = f"<test-{utils.random_string()}@csvbase.com>"
            message.add_header("Message-ID", message_id)
            outbox.enqueue(message)
            message_ids.append(message_id)
        assert len(mock_smtpd.received) == 0
        outbox.flush()
    mock_smtpd.join(5)
    assert list(mock_smtpd.received.keys()) == message_ids
