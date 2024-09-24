"""Add email verification columns

Revision ID: 757b465597b4
Revises: bc116d837946
Create Date: 2024-09-24 12:41:04.535437+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "757b465597b4"
down_revision = "bc116d837946"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "user_emails",
        sa.Column("verification_code", sa.LargeBinary(), nullable=True),
        schema="metadata",
    )
    op.add_column(
        "user_emails",
        sa.Column(
            "verification_code_expiry", sa.DateTime(timezone=True), nullable=True
        ),
        schema="metadata",
    )
    op.add_column(
        "user_emails",
        sa.Column(
            "verification_email_last_sent", sa.DateTime(timezone=True), nullable=True
        ),
        schema="metadata",
    )
    op.add_column(
        "user_emails",
        sa.Column("verified", sa.DateTime(timezone=True), nullable=True),
        schema="metadata",
    )


def downgrade():
    op.drop_column("user_emails", "verified", schema="metadata")
    op.drop_column("user_emails", "verification_email_last_sent", schema="metadata")
    op.drop_column("user_emails", "verification_code_expiry", schema="metadata")
    op.drop_column("user_emails", "verification_code", schema="metadata")
