"""Add mailing list column

Revision ID: 7dd1bbf902b5
Revises: 98e5779863fd
Create Date: 2024-04-25 13:28:38.648512+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7dd1bbf902b5"
down_revision = "98e5779863fd"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column("mailing_list", sa.Boolean(), nullable=True),
        schema="metadata",
    )
    op.execute("update metadata.users set mailing_list = false")
    op.alter_column("users", "mailing_list", nullable=False, schema="metadata")


def downgrade():
    op.drop_column("users", "mailing_list", schema="metadata")
