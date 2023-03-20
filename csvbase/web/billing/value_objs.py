import enum


@enum.unique
class StripeSubscriptionStatus(enum.Enum):
    """Our model of Stripe's own statuses for subscriptions.

    https://stripe.com/docs/api/subscriptions/object#subscription_object-status

    """

    ACTIVE = 1
    PAST_DUE = 2
    UNPAID = 3
    CANCELED = 4
    INCOMPLETE = 5
    INCOMPLETE_EXPIRED = 6
    TRIALING = 7
    PAUSED = 8
