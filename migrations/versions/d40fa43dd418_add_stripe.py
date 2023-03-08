"""Add stripe

Revision ID: d40fa43dd418
Revises: cf3ddc8fb918
Create Date: 2023-03-08 20:53:21.760888+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d40fa43dd418"
down_revision = "cf3ddc8fb918"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payment_references",
        sa.Column(
            "payment_reference_uuid", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_reference", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_payment_references_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint(
            "payment_reference_uuid",
            "user_uuid",
            "payment_reference",
            name=op.f("pk_payment_references"),
        ),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_payment_references_payment_reference"),
        "payment_references",
        ["payment_reference"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_payment_references_payment_reference_uuid"),
        "payment_references",
        ["payment_reference_uuid"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_payment_references_user_uuid"),
        "payment_references",
        ["user_uuid"],
        unique=False,
        schema="metadata",
    )
    op.create_table(
        "stripe_customers",
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stripe_customer_id", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_stripe_customers_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("user_uuid", name=op.f("pk_stripe_customers")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_customers_stripe_customer_id"),
        "stripe_customers",
        ["stripe_customer_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_stripe_customers_user_uuid"),
        "stripe_customers",
        ["user_uuid"],
        unique=False,
        schema="metadata",
    )


def downgrade():
    op.drop_index(
        op.f("ix_metadata_stripe_customers_user_uuid"),
        table_name="stripe_customers",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_stripe_customers_stripe_customer_id"),
        table_name="stripe_customers",
        schema="metadata",
    )
    op.drop_table("stripe_customers", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_payment_references_user_uuid"),
        table_name="payment_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_payment_references_payment_reference_uuid"),
        table_name="payment_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_payment_references_payment_reference"),
        table_name="payment_references",
        schema="metadata",
    )
    op.drop_table("payment_references", schema="metadata")
