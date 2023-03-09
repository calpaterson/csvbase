from uuid import uuid4, UUID
from logging import getLogger

from flask import url_for, Blueprint, redirect, g, flash
import stripe

from ..svc import user_by_user_uuid
from ..sesh import get_sesh
from ..value_objs import User
from .. import models
from ..config import get_config
from . import svc

logger = getLogger(__name__)

bp = Blueprint("billing", __name__)


def init_blueprint(app):
    config = get_config()
    stripe.api_key = config.stripe_api_key
    app.register_blueprint(bp, url_prefix="/billing/")
    logger.info("initialised billing blueprint")


@bp.route("/subscribe", methods=["GET"])
def subscribe():
    """Redirect the user to the checkout"""
    sesh = get_sesh()
    config = get_config()

    price_id = config.stripe_price_id
    current_user: User = g.current_user

    payment_reference_uuid = uuid4()
    payment_ref = models.PaymentReference(
        payment_reference_uuid=payment_reference_uuid, user_uuid=current_user.user_uuid
    )
    sesh.add(payment_ref)

    checkout_session_kwargs = {
        "mode": "subscription",
        "success_url": url_for(
            "billing.success",
            payment_reference_uuid=payment_reference_uuid,
            _external=True,
        ),
        "cancel_url": url_for(
            "billing.cancel",
            payment_reference_uuid=payment_reference_uuid,
            _external=True,
        ),
        "client_reference_id": str(payment_reference_uuid),
        "line_items": [{"price": price_id, "quantity": 1}],
    }
    if current_user.email is not None and "@" in current_user.email:
        checkout_session_kwargs["customer_email"] = current_user.email
    stripe_customer_id = svc.get_stripe_customer_id(sesh, current_user.user_uuid)
    if stripe_customer_id is not None:
        checkout_session_kwargs["customer"] = stripe_customer_id

    checkout_session = stripe.checkout.Session.create(**checkout_session_kwargs)

    logger.info(
        "created checkout session '%s' for '%s'",
        checkout_session.id,
        current_user.username,
    )

    payment_ref.payment_reference = checkout_session.id
    sesh.commit()

    return redirect(checkout_session.url)


@bp.route("/success/<payment_reference_uuid>", methods=["GET"])
def success(payment_reference_uuid: str):
    """The URL that a user gets redirected to upon a successful checkout.

    It's import that this is idempotent as users might refresh.

    """
    sesh = get_sesh()
    user_uuid, payment_reference = svc.get_payment_reference(
        sesh, UUID(payment_reference_uuid)
    )
    checkout_session = stripe.checkout.Session.retrieve(payment_reference)

    logger.info(
        "checkout session succeeded: '%s', status: '%s', payment_status: '%s'",
        checkout_session.id,
        checkout_session.status,
        checkout_session.payment_status,
    )

    svc.insert_stripe_customer_id(sesh, user_uuid, checkout_session.customer)
    sesh.commit()

    user = user_by_user_uuid(sesh, user_uuid)

    flash("You have subscribed to csvbase")
    return redirect(url_for("csvbase.user", username=user.username))


@bp.route("/cancel/<payment_reference_uuid>", methods=["GET"])
def cancel(payment_reference_uuid: str):
    raise NotImplementedError("not implemented")
