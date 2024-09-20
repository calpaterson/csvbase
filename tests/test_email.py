from email.message import EmailMessage
from csvbase import email


def test_email__send_email(mock_smtpd):
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "receiver@example.come"
    message["Subject"] = "Test"
    message.set_content("Test")
    email.send(message)
    mock_smtpd.join()
    assert len(mock_smtpd.received) == 1
