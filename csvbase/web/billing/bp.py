from uuid import uuid4, UUID
from logging import getLogger

from flask import url_for, Blueprint, redirect, g, flash, Flask
from werkzeug.wrappers.response import Response
import stripe

from ...svc import user_by_user_uuid
from ...sesh import get_sesh

# from ..web import am_user_or_400
from ...value_objs import User
from ...config import get_config
from ... import exc
from . import svc

logger = getLogger(__name__)

bp = Blueprint("billing", __name__)


def init_blueprint(app: Flask) -> None:
    config = get_config()
    if config.stripe_api_key is not None:
        stripe.api_key = config.stripe_api_key
        app.register_blueprint(bp, url_prefix="/billing/")
        logger.info("initialised billing blueprint")


@bp.route("/subscribe", methods=["GET"])
def subscribe() -> Response:
    """Redirect the user to the checkout"""
    sesh = get_sesh()
    config = get_config()

    price_id = config.stripe_price_id
    current_user: User = g.current_user

    payment_reference_uuid = uuid4()

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

    # This is an attempt to pre-fill if the email looks real but stripe may
    # still reject and that is handled later
    if current_user.email is not None and "@" in current_user.email:
        checkout_session_kwargs["customer_email"] = current_user.email
    stripe_customer_id = svc.get_stripe_customer_id(sesh, current_user.user_uuid)
    if stripe_customer_id is not None:
        checkout_session_kwargs["customer"] = stripe_customer_id

    try:
        checkout_session = stripe.checkout.Session.create(**checkout_session_kwargs)
    except stripe.error.InvalidRequestError as e:
        if e.code == "email_invalid":
            # if stripe did reject the email address, remove it and retry
            logger.warning(
                "stripe rejected customer email: '%s'",
                checkout_session_kwargs["customer_email"],
            )
            del checkout_session_kwargs["customer_email"]
            checkout_session = stripe.checkout.Session.create(**checkout_session_kwargs)
        else:
            logger.exception("stripe invalid request error")
            raise

    logger.info(
        "created checkout session '%s' for '%s'",
        checkout_session.id,
        current_user.username,
    )

    svc.record_payment_reference(
        sesh, payment_reference_uuid, current_user, checkout_session.id
    )
    sesh.commit()

    return redirect(checkout_session.url)


@bp.route("/success/<payment_reference_uuid>", methods=["GET"])
def success(payment_reference_uuid: str) -> Response:
    """The URL that a user gets redirected to upon a successful checkout.

    It's import that this is idempotent as users might refresh.

    """
    sesh = get_sesh()
    payment_ref_tup = svc.get_payment_reference(sesh, UUID(payment_reference_uuid))
    if payment_ref_tup is None:
        logger.error("no such payment reference uuid: %s", payment_reference_uuid)
        raise exc.UnknownPaymentReferenceUUIDException(payment_reference_uuid)

    user_uuid, payment_reference = payment_ref_tup

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

    # FIXME: now mark the user as having subscribed

    flash("You have subscribed to csvbase")
    return redirect(url_for("csvbase.user", username=user.username))


@bp.route("/cancel/<payment_reference_uuid>", methods=["GET"])
def cancel(payment_reference_uuid: str):
    raise NotImplementedError("not implemented")


@bp.route("/manage", methods=["GET"])
def manage() -> Response:
    # FIXME: needed but cannot be imported due to an import loop
    # am_user_or_400()
    sesh = get_sesh()
    current_user = g.current_user
    customer_id = svc.get_stripe_customer_id(sesh, current_user.user_uuid)
    portal_session = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=url_for(
            "csvbase.user", username=current_user.username, _external=True
        ),
    )
    logger.info(
        "Portal session '%s' created for %s", portal_session.id, current_user.username
    )
    return redirect(portal_session.url)
