from uuid import uuid4, UUID
from logging import getLogger

from flask import url_for, Blueprint, redirect, g
import stripe

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
    app.register_blueprint(bp)
    logger.info("initialised billing blueprint")


@bp.route("/subscribe", methods=["GET"])
def subscribe():
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
        "client_reference_id": str(payment_reference_uuid),
        "line_items": [{"price": price_id, "quantity": 1}],
    }

    if current_user.email is not None:
        checkout_session_kwargs["customer_email"] = current_user.email
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

    # now get
    return "ok"
