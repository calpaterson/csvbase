from csvbase import email

import pytest


@pytest.mark.xfail(reason="not implemented")
def test_email__send_email(mock_smtpd):
    message = None
    email.send("test@example.com", message=message)
    assert len(mock_smtpd.received) == 1
