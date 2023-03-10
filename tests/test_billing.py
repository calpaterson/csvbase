from unittest.mock import patch
from typing import Optional
from uuid import uuid4
from dataclasses import dataclass

from csvbase.web import set_current_user
from csvbase.billing import svc
from csvbase.billing import bp

from .utils import random_string


@dataclass
class FakeCheckoutSession:
    id: str
    status: str
    payment_status: str
    customer: Optional[str] = None
    url: str = "http://example.com/checkout"


def test_subscribe(client, test_user):
    set_current_user(test_user)
    fake_checkout_session = FakeCheckoutSession(
        id=random_string(),
        status="complete",
        payment_status="paid",
    )
    with patch.object(bp.stripe.checkout.Session, "create") as mock_retrieve:
        mock_retrieve.return_value = fake_checkout_session
        subscribe_response = client.get("/billing/subscribe")
    assert subscribe_response.status_code == 302
    assert subscribe_response.headers["Location"] == fake_checkout_session.url


def test_success_url(client, sesh, test_user):
    payment_reference_uuid = uuid4()
    payment_reference = random_string()
    payment_reference_uuid = svc.record_payment_reference(
        sesh, payment_reference_uuid, test_user, payment_reference
    )
    sesh.commit()

    with patch.object(bp.stripe.checkout.Session, "retrieve") as mock_retrieve:
        mock_retrieve.return_value = FakeCheckoutSession(
            id=payment_reference,
            customer=random_string(),
            url=random_string(),
            status="complete",
            payment_status="paid",
        )
        resp = client.get(f"/billing/success/{str(payment_reference_uuid)}")
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}"
