"""Add data licences

Revision ID: 8951426b65be
Revises: a0f88c5755b3
Create Date: 2022-04-21 21:47:28.080908+01:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column


# revision identifiers, used by Alembic.
revision = "8951426b65be"
down_revision = "a0f88c5755b3"
branch_labels = None
depends_on = None

data_licence_table = table(
    "data_licences",
    column("licence_id", sa.SmallInteger),
    column("licence_name", sa.String),
    schema="metadata",
)

data_licences = [
    (0, "UNKNOWN"),
    (1, "ALL_RIGHTS_RESERVED"),
    (2, "PDDL"),
    (3, "ODC_BY"),
    (4, "ODBL"),
    (5, "OGL"),
]


def upgrade():
    op.bulk_insert(
        data_licence_table,
        [{"licence_id": id, "licence_name": name} for id, name in data_licences]
    )


def downgrade():
    op.get_bind().execute("delete from metadata.data_licences")
