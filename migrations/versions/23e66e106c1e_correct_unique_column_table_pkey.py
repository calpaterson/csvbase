"""Correct unique column table pkey

Revision ID: 23e66e106c1e
Revises: 47062cc1c8e0
Create Date: 2024-05-10 10:15:27.928882+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "23e66e106c1e"
down_revision = "47062cc1c8e0"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint(
        "pk_unique_columns", "unique_columns", type_="primary", schema="metadata"
    )
    op.create_primary_key(
        "pk_unique_columns",
        table_name="unique_columns",
        columns=["table_uuid", "column_name"],
        schema="metadata",
    )


def downgrade():
    op.drop_constraint(
        "pk_unique_columns", "unique_columns", type_="primary", schema="metadata"
    )
    op.create_primary_key(
        "pk_unique_columns",
        table_name="unique_columns",
        columns=["table_uuid"],
        schema="metadata",
    )
