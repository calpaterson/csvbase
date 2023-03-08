from typing import Tuple, Optional
from uuid import UUID

from ..models import PaymentReference


def get_payment_reference(sesh, payment_reference_uuid: UUID) -> Tuple[UUID, str]:
    return (
        sesh.query(PaymentReference.user_uuid, PaymentReference.payment_reference)
        .filter(PaymentReference.payment_reference_uuid == payment_reference_uuid)
        .one()
    )


def insert_stripe_customer_id(sesh, user_uuid: UUID, stripe_customer_id: str) -> None:
    ...


def get_stripe_customer_iud(sesh, user_uuid: UUID) -> Optional[str]:
    ...
