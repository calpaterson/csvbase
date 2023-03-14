from typing import Tuple, Optional, Iterable
from uuid import UUID
from logging import getLogger

from sqlalchemy import func
import stripe

from ...config import get_config
from ...models import PaymentReference, StripeCustomer
from ...value_objs import User

logger = getLogger(__name__)

def initialise_stripe() -> None:
    if stripe.api_key is None:
        config = get_config()
        stripe.api_key = config.stripe_api_key


def record_payment_reference(
    sesh, payment_reference_uuid: UUID, user: User, payment_reference: str
) -> None:
    sesh.add(
        PaymentReference(
            payment_reference_uuid=payment_reference_uuid,
            user_uuid=user.user_uuid,
            payment_reference=payment_reference,
        )
    )


def get_payment_reference(
    sesh, payment_reference_uuid: UUID
) -> Optional[Tuple[UUID, str]]:
    return (
        sesh.query(PaymentReference.user_uuid, PaymentReference.payment_reference)
        .filter(PaymentReference.payment_reference_uuid == payment_reference_uuid)
        .one_or_none()
    )


def insert_stripe_customer_id(sesh, user_uuid: UUID, stripe_customer_id: str) -> None:
    sesh.add(StripeCustomer(user_uuid=user_uuid, stripe_customer_id=stripe_customer_id))


def get_stripe_customer_id(sesh, user_uuid: UUID) -> Optional[str]:
    return (
        sesh.query(StripeCustomer.stripe_customer_id)
        .filter(StripeCustomer.user_uuid == user_uuid)
        .scalar()
    )


def has_had_subscription(sesh, user_uuid: UUID) -> bool:
    """This is a temporary function - to be replaced later with a fuller
    conception of subscription statuses."""
    return sesh.query(
        sesh.query(StripeCustomer)
        .filter(StripeCustomer.user_uuid == user_uuid)
        .exists()
    ).scalar()



def get_stripe_subscriptions_for_update(sesh) -> Iterable:
    query = sesh.query(StripeSubscription.stripe_subjection_id).filter(StripeSubscription.ttl<func.now())
    for stripe_subscription_id, in query:
        yield stripe.Subscription.retrieve(stripe_subscription_id)


def update_stripe_subscriptions(sesh, full: bool) -> None:
    stripe_subscription_objs: Iterable
    if not full:
        stripe_subscription_objs = get_stripe_subscriptions_for_update(sesh)
    else:
        stripe_subscription_objs = stripe.Subscription.list()

    for stripe_subscription_obj in stripe_subscription_objs:
        logger.info("checking %s", stripe_subjection_obj.id)
        ...


