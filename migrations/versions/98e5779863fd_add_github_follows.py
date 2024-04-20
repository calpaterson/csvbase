"""Add github follows

Revision ID: 98e5779863fd
Revises: 75a882d6c74e
Create Date: 2024-04-20 23:33:29.522264+01:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "98e5779863fd"
down_revision = "75a882d6c74e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "github_follows",
        sa.Column("table_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("last_sha", postgresql.BYTEA(), nullable=False),
        sa.Column("last_modified", sa.DateTime(timezone=True), nullable=False),
        sa.Column("org", sa.String(), nullable=False),
        sa.Column("repo", sa.String(), nullable=False),
        sa.Column("branch", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["table_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_github_follows_table_uuid_tables"),
        ),
        sa.PrimaryKeyConstraint("table_uuid", name=op.f("pk_github_follows")),
        schema="metadata",
    )


def downgrade():
    op.drop_table("github_follows", schema="metadata")
