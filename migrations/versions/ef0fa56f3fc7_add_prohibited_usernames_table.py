"""Add prohibited usernames table

Revision ID: ef0fa56f3fc7
Revises: 1ec343a3a7bd
Create Date: 2022-05-11 21:03:11.069249+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ef0fa56f3fc7"
down_revision = "1ec343a3a7bd"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "prohibited_usernames",
        sa.Column("username", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("username", name=op.f("pk_prohibited_usernames")),
        schema="metadata",
    )


def downgrade():
    op.drop_table("prohibited_usernames", schema="metadata")
