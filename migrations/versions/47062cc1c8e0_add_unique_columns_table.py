"""Add unique columns table

Revision ID: 47062cc1c8e0
Revises: 5247a5a65c3c
Create Date: 2024-05-03 13:18:24.362186+01:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "47062cc1c8e0"
down_revision = "5247a5a65c3c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "unique_columns",
        sa.Column("table_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("column_name", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["table_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_unique_columns_table_uuid_tables"),
        ),
        sa.PrimaryKeyConstraint("table_uuid", name=op.f("pk_unique_columns")),
        schema="metadata",
    )


def downgrade():
    op.drop_table("unique_columns", schema="metadata")
