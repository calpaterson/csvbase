"""Use a repo_url column

Revision ID: 5247a5a65c3c
Revises: 7dd1bbf902b5
Create Date: 2024-05-02 07:27:51.387374+01:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5247a5a65c3c"
down_revision = "7dd1bbf902b5"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("github_follows", "org", schema="metadata")
    op.alter_column(
        "github_follows", "repo", new_column_name="https_repo_url", schema="metadata"
    )


def downgrade():
    op.add_column(
        "github_follows",
        sa.Column("org", sa.VARCHAR(), autoincrement=False, nullable=False),
        schema="metadata",
    )
    op.alter_column(
        "github_follows", "https_repo_url", new_column_name="repo", schema="metadata"
    )
