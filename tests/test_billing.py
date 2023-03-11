from unittest.mock import patch
from typing import Optional
from uuid import uuid4
from dataclasses import dataclass, field

from csvbase.web.main.bp import set_current_user
from csvbase.web.billing import svc, bp

from .utils import random_string

import pytest


def random_stripe_url(host: str = "stripe.com", path_prefix="/") -> str:
    """Returns realistic (but randomized) stripe urls.

    Useful for asserting that redirects went to the right url.

    """
    return f"https://{host}{path_prefix}/{random_string()}"


@dataclass
class FakeCheckoutSession:
    id: str
    status: str
    payment_status: str
    customer: Optional[str] = None
    url: str = "http://example.com/checkout"


@dataclass
class FakePortalSession:
    url: str
    id: str = field(default_factory=random_string)


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


@pytest.mark.xfail(reason="not implemented")
def test_success_url__payment_reference_does_not_exist():
    assert False


def test_manage__happy(client, sesh, test_user):
    stripe_customer_id = random_string()
    svc.insert_stripe_customer_id(sesh, test_user.user_uuid, stripe_customer_id)
    sesh.commit()

    set_current_user(test_user)
    portal_url = random_stripe_url("billing.stripe.com", path_prefix="/session")
    with patch.object(bp.stripe.billing_portal.Session, "create") as mock_create:
        mock_create.return_value = FakePortalSession(url=portal_url)
        resp = client.get("/billing/manage")
    assert resp.status_code == 302
    assert resp.headers["Location"] == portal_url


@pytest.mark.xfail(reason="not implemented")
def test_manage__no_stripe_customer(client, sesh, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_manage__not_signed_in(client, sesh):
    assert False
