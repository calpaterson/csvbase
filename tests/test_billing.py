from csvbase.web import set_current_user

from dataclasses import dataclass


@dataclass
class FakeCheckoutSession:
    id: str
    customer: str
    url: str
    status: str
    payment_status: str


def test_billing_success(client, test_user):
    set_current_user(test_user)
    subscribe_response = client.get("/subscribe")
    assert subscribe_response.status_code == 302
