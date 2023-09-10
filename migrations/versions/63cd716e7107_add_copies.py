"""Add copies

Revision ID: 63cd716e7107
Revises: 3c8dab82577e
Create Date: 2023-09-10 22:12:50.520564+01:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "63cd716e7107"
down_revision = "3c8dab82577e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "copies",
        sa.Column(
            "copy_id", sa.BigInteger(), sa.Identity(always=False), nullable=False  # type: ignore
        ),
        sa.Column("from_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("to_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["from_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_copies_from_uuid_tables"),
        ),
        sa.ForeignKeyConstraint(
            ["to_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_copies_to_uuid_tables"),
        ),
        sa.PrimaryKeyConstraint("from_uuid", "to_uuid", name=op.f("pk_copies")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_copies_copy_id"),
        "copies",
        ["copy_id"],
        unique=True,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_copies_created"),
        "copies",
        ["created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_copies_from_uuid"),
        "copies",
        ["from_uuid"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_copies_to_uuid"),
        "copies",
        ["to_uuid"],
        unique=False,
        schema="metadata",
    )


def downgrade():
    op.drop_index(
        op.f("ix_metadata_copies_to_uuid"), table_name="copies", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_copies_from_uuid"), table_name="copies", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_copies_created"), table_name="copies", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_copies_copy_id"), table_name="copies", schema="metadata"
    )
    op.drop_table("copies", schema="metadata")
