"""Add praise table

Revision ID: 1ec343a3a7bd
Revises: 8951426b65be
Create Date: 2022-04-27 14:49:59.214171+01:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "1ec343a3a7bd"
down_revision = "8951426b65be"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "praise",
        sa.Column(
            "praise_id", sa.BigInteger(), sa.Identity(always=False), nullable=False  # type: ignore
        ),
        sa.Column("table_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "praised",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["table_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_praise_table_uuid_tables"),
        ),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_praise_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("praise_id", name=op.f("pk_praise")),
        sa.UniqueConstraint(
            "user_uuid", "table_uuid", name=op.f("uq_praise_user_uuid")
        ),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_praise_praised"),
        "praise",
        ["praised"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_praise_table_uuid"),
        "praise",
        ["table_uuid"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_praise_user_uuid"),
        "praise",
        ["user_uuid"],
        unique=False,
        schema="metadata",
    )


def downgrade():
    op.drop_index(
        op.f("ix_metadata_praise_user_uuid"), table_name="praise", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_praise_table_uuid"), table_name="praise", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_praise_praised"), table_name="praise", schema="metadata"
    )
    op.drop_table("praise", schema="metadata")
