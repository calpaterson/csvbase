"""Initial migration

Revision ID: a0f88c5755b3
Revises:
Create Date: 2022-04-21 21:44:51.737816+01:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a0f88c5755b3"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "data_licences",
        sa.Column("licence_id", sa.SmallInteger(), autoincrement=False, nullable=False),
        sa.Column("licence_name", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("licence_id", name=op.f("pk_data_licences")),
        schema="metadata",
    )
    op.create_table(
        "users",
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("username", sa.String(), nullable=False),
        sa.Column("password_hash", sa.String(), nullable=False),
        sa.Column("timezone", sa.String(), nullable=False),
        sa.Column("registered", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "username ~ '^[A-z][-A-z0-9]+$'", name=op.f("ck_users_username_format")
        ),
        sa.CheckConstraint(
            "char_length(username) <= 200", name=op.f("ck_users_username_length")
        ),
        sa.PrimaryKeyConstraint("user_uuid", name=op.f("pk_users")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_users_username"),
        "users",
        ["username"],
        unique=True,
        schema="metadata",
    )
    op.create_table(
        "api_keys",
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("api_key", postgresql.BYTEA(length=16), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_api_keys_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("user_uuid", name=op.f("pk_api_keys")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_api_keys_api_key"),
        "api_keys",
        ["api_key"],
        unique=True,
        schema="metadata",
    )
    op.create_table(
        "tables",
        sa.Column("table_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("public", sa.Boolean(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("licence_id", sa.SmallInteger(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("caption", sa.String(), nullable=False),
        sa.CheckConstraint(
            "table_name ~ '^[A-z][-A-z0-9]+$'", name=op.f("ck_tables_table_name_format")
        ),
        sa.CheckConstraint(
            "char_length(caption) <= 200", name=op.f("ck_tables_caption_length")
        ),
        sa.CheckConstraint(
            "char_length(table_name) <= 200", name=op.f("ck_tables_table_name_length")
        ),
        sa.ForeignKeyConstraint(
            ["licence_id"],
            ["metadata.data_licences.licence_id"],
            name=op.f("fk_tables_licence_id_data_licences"),
        ),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_tables_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("table_uuid", name=op.f("pk_tables")),
        sa.UniqueConstraint(
            "user_uuid", "table_name", name=op.f("uq_tables_user_uuid")
        ),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_tables_created"),
        "tables",
        ["created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_tables_table_name"),
        "tables",
        ["table_name"],
        unique=False,
        schema="metadata",
    )
    op.create_table(
        "user_emails",
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("email_address", sa.String(length=200), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_user_emails_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("user_uuid", name=op.f("pk_user_emails")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_user_emails_email_address"),
        "user_emails",
        ["email_address"],
        unique=False,
        schema="metadata",
    )
    op.create_table(
        "table_readmes",
        sa.Column("table_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("readme_markdown", sa.String(length=10000), nullable=False),
        sa.ForeignKeyConstraint(
            ["table_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_table_readmes_table_uuid_tables"),
        ),
        sa.PrimaryKeyConstraint("table_uuid", name=op.f("pk_table_readmes")),
        schema="metadata",
    )


def downgrade():
    op.drop_table("table_readmes", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_user_emails_email_address"),
        table_name="user_emails",
        schema="metadata",
    )
    op.drop_table("user_emails", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_tables_table_name"), table_name="tables", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_tables_created"), table_name="tables", schema="metadata"
    )
    op.drop_table("tables", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_api_keys_api_key"), table_name="api_keys", schema="metadata"
    )
    op.drop_table("api_keys", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_users_username"), table_name="users", schema="metadata"
    )
    op.drop_table("users", schema="metadata")
    op.drop_table("data_licences", schema="metadata")
