from typing import Tuple, Optional
from uuid import UUID

from ..models import PaymentReference
from ..value_objs import User


def record_payment_reference(
    sesh, payment_reference_uuid: UUID, user: User, payment_reference: str
) -> UUID:
    sesh.add(
        PaymentReference(
            payment_reference_uuid=payment_reference_uuid,
            user_uuid=user.user_uuid,
            payment_reference=payment_reference,
        )
    )
    return payment_reference_uuid


def get_payment_reference(sesh, payment_reference_uuid: UUID) -> Tuple[UUID, str]:
    return (
        sesh.query(PaymentReference.user_uuid, PaymentReference.payment_reference)
        .filter(PaymentReference.payment_reference_uuid == payment_reference_uuid)
        .one()
    )


def insert_stripe_customer_id(sesh, user_uuid: UUID, stripe_customer_id: str) -> None:
    ...


def get_stripe_customer_id(sesh, user_uuid: UUID) -> Optional[str]:
    ...
