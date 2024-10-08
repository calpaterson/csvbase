"""Add stripe subscription

Revision ID: 3c8dab82577e
Revises: 878b845f7368
Create Date: 2023-03-20 15:35:52.502929+00:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import table, column

# revision identifiers, used by Alembic.
revision = "3c8dab82577e"
down_revision = "878b845f7368"
branch_labels = None
depends_on = None

subscription_statuses_table = table(
    "stripe_subscription_statuses",
    column("stripe_subscription_status_id", sa.SmallInteger),
    column("stripe_subscription_status", sa.String),
    schema="metadata",
)


stripe_subscription_statuses = {
    "ACTIVE": 1,
    "PAST_DUE": 2,
    "UNPAID": 3,
    "CANCELED": 4,
    "INCOMPLETE": 5,
    "INCOMPLETE_EXPIRED": 6,
    "TRIALING": 7,
    "PAUSED": 8,
}


def upgrade():
    op.create_table(
        "stripe_subscription_statuses",
        sa.Column(
            "stripe_subscription_status_id",
            sa.SmallInteger(),
            nullable=False,
            autoincrement=False,
        ),
        sa.Column("stripe_subscription_status", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint(
            "stripe_subscription_status_id",
            name=op.f("pk_stripe_subscription_statuses"),
        ),
        sa.UniqueConstraint(
            "stripe_subscription_status",
            name=op.f("uq_stripe_subscription_statuses_stripe_subscription_status"),
        ),
        sa.UniqueConstraint(
            "stripe_subscription_status_id",
            name=op.f("uq_stripe_subscription_statuses_stripe_subscription_status_id"),
        ),
        schema="metadata",
    )
    op.create_table(
        "stripe_subscriptions",
        sa.Column("stripe_subscription_id", sa.String(), nullable=False),
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stripe_subscription_status_id", sa.SmallInteger(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ttl", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["stripe_subscription_status_id"],
            ["metadata.stripe_subscription_statuses.stripe_subscription_status_id"],
            name=op.f(
                "fk_stripe_subscriptions_stripe_subscription_status_id_stripe_subscription_statuses"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_stripe_subscriptions_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint(
            "stripe_subscription_id", name=op.f("pk_stripe_subscriptions")
        ),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_subscriptions_created"),
        "stripe_subscriptions",
        ["created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_subscriptions_stripe_subscription_status_id"),
        "stripe_subscriptions",
        ["stripe_subscription_status_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_subscriptions_ttl"),
        "stripe_subscriptions",
        ["ttl"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_subscriptions_updated"),
        "stripe_subscriptions",
        ["updated"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_subscriptions_user_uuid"),
        "stripe_subscriptions",
        ["user_uuid"],
        unique=False,
        schema="metadata",
    )
    op.bulk_insert(
        subscription_statuses_table,
        [
            {"stripe_subscription_status_id": id, "stripe_subscription_status": status}
            for status, id in stripe_subscription_statuses.items()
        ],
    )


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_metadata_stripe_subscriptions_user_uuid"),
        table_name="stripe_subscriptions",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_stripe_subscriptions_updated"),
        table_name="stripe_subscriptions",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_stripe_subscriptions_ttl"),
        table_name="stripe_subscriptions",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_stripe_subscriptions_stripe_subscription_status_id"),
        table_name="stripe_subscriptions",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_stripe_subscriptions_created"),
        table_name="stripe_subscriptions",
        schema="metadata",
    )
    op.drop_table("stripe_subscriptions", schema="metadata")
    op.drop_table("stripe_subscription_statuses", schema="metadata")
