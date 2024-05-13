from unittest.mock import patch
from uuid import uuid4

import sqlalchemy

from csvbase.svc import create_user
from csvbase.web.func import set_current_user
from csvbase.web.billing import svc, bp

from .utils import (
    random_string,
    make_user,
    FakeStripeSubscription,
    FakeStripePortalSession,
    FakeStripeCheckoutSession,
)

import stripe
import pytest


def test_subscribe(client, test_user):
    set_current_user(test_user)
    fake_checkout_session = FakeStripeCheckoutSession(
        status="complete",
        payment_status="paid",
    )
    with patch.object(bp.stripe.checkout.Session, "create") as mock_create:
        mock_create.return_value = fake_checkout_session
        subscribe_response = client.get("/billing/subscribe")
    assert subscribe_response.status_code == 302
    assert subscribe_response.headers["Location"] == fake_checkout_session.url


@pytest.mark.xfail(reason="not implemented")
def test_subscribe__stripe_customer_already_exists(client, test_user):
    assert False


def test_subscribe__stripe_rejects_customer_email(client, sesh, app):
    crypt_context = app.config["CRYPT_CONTEXT"]
    test_user = create_user(
        sesh, crypt_context, random_string(), random_string(), email="darth@deathstar"
    )
    sesh.commit()
    set_current_user(test_user)
    fake_checkout_session = FakeStripeCheckoutSession(
        id=random_string(),
        status="complete",
        payment_status="paid",
    )

    def reject_if_email_present(**kwargs):
        if "customer_email" in kwargs:
            raise stripe.error.InvalidRequestError(
                message="Invalid email address: darth@deathstar",
                param="customer_email",
                code="email_invalid",
                http_status=400,
            )
        else:
            return fake_checkout_session

    with patch.object(bp.stripe.checkout.Session, "create") as mock_create:
        mock_create.side_effect = reject_if_email_present
        subscribe_response = client.get("/billing/subscribe")
    assert subscribe_response.status_code == 302
    assert subscribe_response.headers["Location"] == fake_checkout_session.url


def test_subscribe__not_signed_in(client):
    """When you try to subscribe but aren't signed in you get asked to register"""
    subscribe_response = client.get(
        "/billing/subscribe", headers={"Accept": "text/html"}
    )
    assert subscribe_response.status_code == 302
    assert subscribe_response.headers["Location"] == "/register"


def test_subscribe__stripe_rejects_for_other_reason(client, test_user):
    set_current_user(test_user)
    with patch.object(bp.stripe.checkout.Session, "create") as mock_create:
        mock_create.side_effect = stripe.error.InvalidRequestError(
            message="Something else",
            param="something",
        )
        with pytest.raises(stripe.error.InvalidRequestError):
            client.get("/billing/subscribe")


def test_success_url(client, sesh, test_user):
    payment_reference_uuid = uuid4()
    payment_reference = random_string()
    svc.record_payment_reference(
        sesh, payment_reference_uuid, test_user, payment_reference
    )
    sesh.commit()

    with patch.object(bp.stripe.checkout.Session, "retrieve") as mock_retrieve:
        mock_retrieve.return_value = FakeStripeCheckoutSession(
            id=payment_reference,
            customer=random_string(),
            url=random_string(),
            status="complete",
            payment_status="paid",
        )
        resp = client.get(f"/billing/success/{str(payment_reference_uuid)}")

    assert svc.get_stripe_customer_id(sesh, test_user.user_uuid) is not None

    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}"


def test_success_url__is_idempotent(client, sesh, test_user):
    payment_reference_uuid = uuid4()
    payment_reference = random_string()
    svc.record_payment_reference(
        sesh, payment_reference_uuid, test_user, payment_reference
    )
    sesh.commit()

    with patch.object(bp.stripe.checkout.Session, "retrieve") as mock_retrieve:
        mock_retrieve.return_value = FakeStripeCheckoutSession(
            id=payment_reference,
            customer=random_string(),
            url=random_string(),
            status="complete",
            payment_status="paid",
        )
        resp1 = client.get(f"/billing/success/{str(payment_reference_uuid)}")
        resp2 = client.get(f"/billing/success/{str(payment_reference_uuid)}")

    assert svc.get_stripe_customer_id(sesh, test_user.user_uuid) is not None

    assert resp1.status_code == 302
    assert resp1.headers["Location"] == f"/{test_user.username}"

    assert resp2.status_code == 302
    assert resp2.headers["Location"] == f"/{test_user.username}"


def test_success_url__payment_reference_does_not_exist(client):
    resp = client.get(f"/billing/success/{str(uuid4())}")
    assert resp.status_code == 404


def test_manage__happy(client, sesh, test_user):
    stripe_customer_id = random_string()
    svc.insert_stripe_customer_id(sesh, test_user.user_uuid, stripe_customer_id)
    sesh.commit()

    set_current_user(test_user)
    portal_session = FakeStripePortalSession()
    with patch.object(bp.stripe.billing_portal.Session, "create") as mock_create:
        mock_create.return_value = portal_session
        resp = client.get("/billing/manage")
    assert resp.status_code == 302
    assert resp.headers["Location"] == portal_session.url


@pytest.mark.xfail(reason="not implemented")
def test_manage__no_stripe_customer(client, sesh, test_user):
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_manage__not_signed_in(client, sesh):
    assert False


def test_pricing__signed_out(client):
    pricing_resp = client.get("/billing/pricing")
    assert pricing_resp.status_code == 200


def test_pricing__signed_in_but_no_subscription(client, test_user):
    set_current_user(test_user)
    pricing_resp = client.get("/billing/pricing")
    assert pricing_resp.status_code == 200


@pytest.mark.xfail(reason="not implemented")
def test_pricing__signed_in_with_subscription(client, test_user):
    set_current_user(test_user)
    assert False


def test_insert_stripe_customer_id__if_exists_under_different_user(sesh, app):
    """Check this specific issue results in an error"""
    user1 = make_user(sesh, app.config["CRYPT_CONTEXT"])
    user2 = make_user(sesh, app.config["CRYPT_CONTEXT"])

    stripe_customer_id = random_string()

    svc.insert_stripe_customer_id(sesh, user1.user_uuid, stripe_customer_id)
    sesh.commit()

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        svc.insert_stripe_customer_id(sesh, user2.user_uuid, stripe_customer_id)
        sesh.commit()


def test_insert_stripe_subscription_id__if_exists_under_different_user(sesh, app):
    """Check this specific issue results in an error"""
    user1 = make_user(sesh, app.config["CRYPT_CONTEXT"])
    user2 = make_user(sesh, app.config["CRYPT_CONTEXT"])

    fake_subscription = FakeStripeSubscription()

    svc.insert_stripe_subscription(sesh, user1.user_uuid, fake_subscription)
    sesh.commit()

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        svc.insert_stripe_subscription(sesh, user2.user_uuid, fake_subscription)
        sesh.commit()


def test_cancel(client, sesh, test_user):
    payment_reference_uuid = uuid4()
    payment_reference = random_string()
    svc.record_payment_reference(
        sesh, payment_reference_uuid, test_user, payment_reference
    )
    sesh.commit()

    cancel_resp = client.get(f"/billing/cancel/{payment_reference_uuid}")
    assert cancel_resp.status_code == 302

    assert cancel_resp.headers["Location"] == "/about"
