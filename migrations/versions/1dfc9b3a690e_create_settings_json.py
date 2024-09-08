"""Create settings json

Revision ID: 1dfc9b3a690e
Revises: 5d8f357eca61
Create Date: 2024-09-08 12:40:49.583955+01:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "1dfc9b3a690e"
down_revision = "5d8f357eca61"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column("settings", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema="metadata",
    )
    op.execute(
        """
    UPDATE
       metadata.users
    SET
        settings = jsonb_build_object('timezone', timezone, 'mailing_list', mailing_list)
    """
    )
    op.alter_column("users", "settings", nullable=False, schema="metadata")
    op.drop_column("users", "mailing_list", schema="metadata")
    op.drop_column("users", "timezone", schema="metadata")


def downgrade():
    op.add_column(
        "users",
        sa.Column("timezone", sa.VARCHAR(), autoincrement=False, nullable=True),
        schema="metadata",
    )
    op.add_column(
        "users",
        sa.Column("mailing_list", sa.BOOLEAN(), autoincrement=False, nullable=True),
        schema="metadata",
    )
    op.execute(
        """
UPDATE metadata.users
SET timezone = settings->>'timezone',
    mailing_list = (settings->>'mailing_list')::boolean;
"""
    )
    op.alter_column("users", "mailing_list", nullable=False, schema="metadata")
    op.alter_column("users", "timezone", nullable=False, schema="metadata")
    op.drop_column("users", "settings", schema="metadata")
