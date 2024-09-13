"""Created licences table

Revision ID: cb79e639ef74
Revises: 1dfc9b3a690e
Create Date: 2024-09-13 11:21:42.681121+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cb79e639ef74"
down_revision = "1dfc9b3a690e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "licences",
        sa.Column("licence_id", sa.SmallInteger(), autoincrement=True, nullable=False),
        sa.Column("spdx_id", sa.String(), nullable=False),
        sa.Column("licence_name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("licence_id", name=op.f("pk_licences")),
        sa.UniqueConstraint("spdx_id", name=op.f("uq_licences_spdx_id")),
        schema="metadata",
    )


def downgrade():
    op.drop_table("licences", schema="metadata")
