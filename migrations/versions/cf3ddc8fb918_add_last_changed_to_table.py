"""Add last_changed to table

Revision ID: cf3ddc8fb918
Revises: ef0fa56f3fc7
Create Date: 2023-01-14 10:14:01.637543+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "cf3ddc8fb918"
down_revision = "ef0fa56f3fc7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tables",
        sa.Column("last_changed", sa.DateTime(timezone=True)),
        schema="metadata",
    )
    op.execute("UPDATE metadata.tables SET last_changed = created")
    op.alter_column("tables", "last_changed", nullable=False, schema="metadata")


def downgrade():
    op.drop_column("tables", "last_changed", schema="metadata")
