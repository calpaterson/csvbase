from typing import Tuple, Optional
from uuid import UUID

from ...models import PaymentReference, StripeCustomer
from ...value_objs import User


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
    ...


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
