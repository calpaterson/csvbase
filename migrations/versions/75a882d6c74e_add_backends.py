"""Add backends

Revision ID: 75a882d6c74e
Revises: cb67ce467141
Create Date: 2024-04-16 10:44:10.475647+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "75a882d6c74e"
down_revision = "cb67ce467141"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "table_backends",
        sa.Column("backend_id", sa.SmallInteger(), autoincrement=False, nullable=False),
        sa.Column("backend_name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("backend_id", name=op.f("pk_table_backends")),
        schema="metadata",
    )
    op.execute(
        "INSERT INTO metadata.table_backends (backend_id, backend_name) VALUES (1, 'postgres');"
    )
    op.add_column(
        "tables",
        sa.Column("backend_id", sa.SmallInteger(), nullable=True),
        schema="metadata",
    )
    op.execute("UPDATE metadata.tables SET backend_id = 1")
    op.alter_column("tables", "backend_id", schema="metadata", nullable=False)
    op.create_foreign_key(
        op.f("fk_tables_backend_id_table_backends"),
        "tables",
        "table_backends",
        ["backend_id"],
        ["backend_id"],
        source_schema="metadata",
        referent_schema="metadata",
    )


def downgrade():
    op.drop_constraint(
        op.f("fk_tables_backend_id_table_backends"),
        "tables",
        schema="metadata",
        type_="foreignkey",
    )
    op.drop_column("tables", "backend_id", schema="metadata")
    op.drop_table("table_backends", schema="metadata")
