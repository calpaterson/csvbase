from typing import Tuple, Optional, Iterable
from datetime import timedelta, datetime, timezone
from uuid import UUID
from logging import getLogger

from sqlalchemy import func
from sqlalchemy.orm import Session
import stripe
from sqlalchemy.dialects.postgresql import insert

from ...config import get_config
from ...models import PaymentReference, StripeCustomer, StripeSubscription
from ...value_objs import User, Quota
from .value_objs import StripeSubscriptionStatus

logger = getLogger(__name__)


def initialise_stripe() -> None:
    """Set the Stripe API key from the config."""
    if stripe.api_key is None:
        config = get_config()
        stripe.api_key = config.stripe_api_key


def record_payment_reference(
    sesh: Session, payment_reference_uuid: UUID, user: User, payment_reference: str
) -> None:
    sesh.add(
        PaymentReference(
            payment_reference_uuid=payment_reference_uuid,
            user_uuid=user.user_uuid,
            payment_reference=payment_reference,
        )
    )


def get_payment_reference(
    sesh: Session, payment_reference_uuid: UUID
) -> Optional[Tuple[UUID, str]]:
    return (
        sesh.query(PaymentReference.user_uuid, PaymentReference.payment_reference)
        .filter(PaymentReference.payment_reference_uuid == payment_reference_uuid)
        .one_or_none()
    )


def insert_stripe_customer_id(
    sesh: Session, user_uuid: UUID, stripe_customer_id: str
) -> None:
    """Insert a stripe customer (idempotently, but raising an
    IntegrityError if it already exists under a different user.

    This is necessary to be defensive if users reload the success url.

    """
    insert_stmt = insert(StripeCustomer).values(
        user_uuid=user_uuid, stripe_customer_id=stripe_customer_id
    )
    if_not_exists_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=["user_uuid"]
    )
    sesh.execute(if_not_exists_stmt)


def get_stripe_customer_id(sesh: Session, user_uuid: UUID) -> Optional[str]:
    return (
        sesh.query(StripeCustomer.stripe_customer_id)
        .filter(StripeCustomer.user_uuid == user_uuid)
        .scalar()
    )


def has_stripe_customer(sesh: Session, user_uuid: UUID) -> bool:
    return sesh.query(
        sesh.query(StripeCustomer)
        .filter(StripeCustomer.user_uuid == user_uuid)
        .exists()
    ).scalar()


def insert_stripe_subscription(
    sesh: Session, user_uuid: UUID, stripe_subscription
) -> None:
    """Insert a stripe subscription (idempotently, but raising an
    IntegrityError if it already exists under a different user.

    This is necessary to be defensive if users reload the success url.

    """
    sub_orm_obj = (
        sesh.query(StripeSubscription)
        .filter(
            StripeSubscription.user_uuid == user_uuid,
            StripeSubscription.stripe_subscription_id == stripe_subscription.id,
        )
        .one_or_none()
    )
    if sub_orm_obj is None:
        sub_orm_obj = StripeSubscription(
            user_uuid=user_uuid, stripe_subscription_id=stripe_subscription.id
        )
        sesh.add(sub_orm_obj)
    fill_stripe_subscription(sub_orm_obj, stripe_subscription)


def get_stripe_subscriptions_for_update(sesh: Session) -> Iterable:
    query = sesh.query(StripeSubscription.stripe_subscription_id).filter(
        StripeSubscription.ttl < func.now(),
        StripeSubscription.stripe_subscription_id.startswith("sub_"),
    )
    for (stripe_subscription_id,) in query:
        yield stripe.Subscription.retrieve(stripe_subscription_id)


def update_stripe_subscription(sesh: Session, stripe_subscription) -> bool:
    stripe_subscription_id = stripe_subscription.id

    sub_orm_obj = sesh.query(StripeSubscription).get(stripe_subscription_id)
    if sub_orm_obj is not None:
        logger.info("updated %s", stripe_subscription_id)
        fill_stripe_subscription(sub_orm_obj, stripe_subscription)
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
            sub_orm_obj = StripeSubscription(
                stripe_subscription_id=stripe_subscription_id,
                user_uuid=customer_obj.user_uuid,
            )
            fill_stripe_subscription(sub_orm_obj, stripe_subscription)
            sesh.add(sub_orm_obj)
        else:
            # resolution of this is manual
            logger.error("did not find %s!", stripe_subscription_id)
            return False
    return True


def fill_stripe_subscription(
    stripe_subscription_orm_obj: StripeSubscription, stripe_subscription_api_obj
) -> None:
    status_id = StripeSubscriptionStatus[
        stripe_subscription_api_obj.status.upper()
    ].value
    ttl = datetime.fromtimestamp(
        stripe_subscription_api_obj.current_period_end, timezone.utc
    ) + timedelta(days=3)
    stripe_subscription_orm_obj.stripe_subscription_status_id = status_id
    stripe_subscription_orm_obj.ttl = ttl


def update_stripe_subscriptions(sesh: Session, full: bool) -> bool:
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


def has_subscription(sesh: Session, user_uuid: UUID) -> bool:
    # FIXME: we generously assume all states are ok for now
    return sesh.query(
        sesh.query(StripeSubscription)
        .filter(StripeSubscription.user_uuid == user_uuid)
        .exists()
    ).scalar()


# by default, users get 100 mb (non-SI units to be generous) of private bytes
DEFAULT_PRIVATE_BYTES = 100 * 1024 * 1024 * 1024

# and a single private table
DEFAULT_PRIVATE_TABLES = 1

# a soft limit of 10 gb (again, non-SI) of private bytes
SUBSCRIBED_PRIVATE_BYTES = 10000 * 1024 * 1024 * 1024

# a soft limit - 1k private tables for subscribed users
SUBSCRIBED_PRIVATE_TABLES = 1000


def get_quota(sesh: Session, user_uuid: UUID) -> Quota:
    if has_subscription(sesh, user_uuid):
        return Quota(
            private_tables=SUBSCRIBED_PRIVATE_TABLES,
            private_bytes=SUBSCRIBED_PRIVATE_BYTES,
        )
    else:
        return Quota(
            private_tables=DEFAULT_PRIVATE_TABLES, private_bytes=DEFAULT_PRIVATE_BYTES
        )
