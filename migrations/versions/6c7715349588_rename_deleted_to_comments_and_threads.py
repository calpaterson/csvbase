"""Rename deleted to comments and threads

Revision ID: 6c7715349588
Revises: 6d59d431ee77
Create Date: 2024-08-27 06:49:41.937417+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6c7715349588"
down_revision = "6d59d431ee77"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "comments",
        sa.Column("deleted", sa.DateTime(timezone=True), nullable=True),
        schema="metadata",
    )
    op.add_column(
        "threads",
        sa.Column("deleted", sa.DateTime(timezone=True), nullable=True),
        schema="metadata",
    )


def downgrade():
    op.drop_column("threads", "deleted", schema="metadata")
    op.drop_column("comments", "deleted", schema="metadata")
