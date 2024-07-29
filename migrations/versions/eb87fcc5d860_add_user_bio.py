"""Add user bio

Revision ID: eb87fcc5d860
Revises: 173e920c9600
Create Date: 2024-07-29 13:12:37.007565+01:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "eb87fcc5d860"
down_revision = "173e920c9600"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "user_bios",
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_bio_markdown", sa.String(length=10000), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_user_bios_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("user_uuid", name=op.f("pk_user_bios")),
        schema="metadata",
    )


def downgrade():
    op.drop_table("user_bios", schema="metadata")
