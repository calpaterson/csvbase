"""Add table licences

Revision ID: bc116d837946
Revises: cb79e639ef74
Create Date: 2024-09-17 12:24:51.691549+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "bc116d837946"
down_revision = "cb79e639ef74"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "table_licences",
        sa.Column("table_uuid", sa.UUID(), nullable=False),
        sa.Column("licence_id", sa.SmallInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["licence_id"],
            ["metadata.licences.licence_id"],
            name=op.f("fk_table_licences_licence_id_licences"),
        ),
        sa.ForeignKeyConstraint(
            ["table_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_table_licences_table_uuid_tables"),
        ),
        sa.PrimaryKeyConstraint(
            "table_uuid", "licence_id", name=op.f("pk_table_licences")
        ),
        sa.UniqueConstraint("table_uuid", name=op.f("uq_table_licences_table_uuid")),
        schema="metadata",
    )
    op.drop_constraint(
        "fk_tables_licence_id_data_licences",
        "tables",
        type_="foreignkey",
        schema="metadata",
    )
    op.alter_column(
        "tables",
        "licence_id",
        existing_type=sa.SMALLINT(),
        nullable=True,
        schema="metadata",
    )


def downgrade():
    op.drop_table("table_licences", schema="metadata")
    op.alter_column(
        "tables",
        "licence_id",
        existing_type=sa.SMALLINT(),
        nullable=False,
        schema="metadata",
    )
    op.create_foreign_key(
        "fk_tables_licence_id_data_licences",
        "tables",
        "data_licences",
        ["licence_id"],
        ["licence_id"],
        source_schema="metadata",
        referent_schema="metadata",
    )
