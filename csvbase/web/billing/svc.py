from typing import Tuple, Optional, Iterable
from datetime import timedelta, datetime, timezone
from uuid import UUID
from logging import getLogger

from sqlalchemy import func
import stripe

from ...config import get_config
from ...models import PaymentReference, StripeCustomer, StripeSubscription
from ...value_objs import User
from .value_objs import StripeSubscriptionStatus

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
    query = sesh.query(StripeSubscription.stripe_subscription_id).filter(
        StripeSubscription.ttl < func.now(),
        StripeSubscription.stripe_subscription_id.startswith("sub_"),
    )
    for (stripe_subscription_id,) in query:
        yield stripe.Subscription.retrieve(stripe_subscription_id)


def update_stripe_subscription(sesh, stripe_subscription) -> bool:
    stripe_subscription_id = stripe_subscription.id
    status_id = StripeSubscriptionStatus[stripe_subscription.status.upper()].value
    ttl = datetime.fromtimestamp(
        stripe_subscription.current_period_end, timezone.utc
    ) + timedelta(days=3)

    sub_orm_obj = sesh.query(StripeSubscription).get(stripe_subscription_id)
    if sub_orm_obj is not None:
        logger.info("updated %s", stripe_subscription_id)
        sub_orm_obj.stripe_subscription_status_id = status_id
        sub_orm_obj.ttl = ttl
    else:
        customer_obj = (
            sesh.query(StripeCustomer)
            .filter(StripeCustomer.stripe_customer_id == stripe_subscription.customer)
            .one_or_none()
        )
        if customer_obj is not None:
            logger.warning(
                "updated up %s (via stripe customer)", stripe_subscription_id
            )
            sesh.add(
                StripeSubscription(
                    stripe_subscription_id=stripe_subscription_id,
                    user_uuid=customer_obj.user_uuid,
                    stripe_subscription_status_id=status_id,
                    ttl=ttl,
                )
            )
        else:
            # resolution of this is manual
            logger.error("did not find %s!", stripe_subscription_id)
            return False
    return True


def update_stripe_subscriptions(sesh, full: bool) -> bool:
    stripe_subscriptions: Iterable
    if not full:
        stripe_subscriptions = get_stripe_subscriptions_for_update(sesh)
    else:
        stripe_subscriptions = stripe.Subscription.list()

    all_updated = True
    for stripe_subscription in stripe_subscriptions:
        all_updated = update_stripe_subscription(sesh, stripe_subscription)
    sesh.commit()
    return all_updated
