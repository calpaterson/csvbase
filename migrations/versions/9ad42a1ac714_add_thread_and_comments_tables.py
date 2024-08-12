"""Add thread and comments tables

Revision ID: 9ad42a1ac714
Revises: 23e66e106c1e
Create Date: 2024-06-03 11:36:05.690559+01:00

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "9ad42a1ac714"
down_revision = "23e66e106c1e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "threads",
        sa.Column(
            "thread_id", sa.BigInteger(), sa.Identity(always=False), nullable=False
        ),
        sa.Column("thread_created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("thread_updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("thread_title", sa.String(), nullable=False),
        sa.CheckConstraint(
            "char_length(thread_title) <= 500", name=op.f("ck_threads_title_length")
        ),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_threads_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("thread_id", name=op.f("pk_threads")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_threads_thread_created"),
        "threads",
        ["thread_created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_threads_thread_updated"),
        "threads",
        ["thread_updated"],
        unique=False,
        schema="metadata",
    )
    op.create_table(
        "comments",
        sa.Column(
            "comment_id", sa.BigInteger(), sa.Identity(always=False), nullable=False
        ),
        sa.Column("user_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("thread_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("comment_updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("comment_markdown", sa.String(), nullable=False),
        sa.CheckConstraint(
            "char_length(comment_markdown) <= 4000",
            name=op.f("ck_comments_markdown_length"),
        ),
        sa.ForeignKeyConstraint(
            ["thread_id"],
            ["metadata.threads.thread_id"],
            name=op.f("fk_comments_thread_id_threads"),
        ),
        sa.ForeignKeyConstraint(
            ["user_uuid"],
            ["metadata.users.user_uuid"],
            name=op.f("fk_comments_user_uuid_users"),
        ),
        sa.PrimaryKeyConstraint("comment_id", name=op.f("pk_comments")),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comments_comment_created"),
        "comments",
        ["comment_created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comments_comment_updated"),
        "comments",
        ["comment_updated"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comments_thread_id"),
        "comments",
        ["thread_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comments_user_uuid"),
        "comments",
        ["user_uuid"],
        unique=False,
        schema="metadata",
    )


def downgrade():
    op.drop_index(
        op.f("ix_metadata_comments_user_uuid"), table_name="comments", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_comments_thread_id"), table_name="comments", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_comments_comment_updated"),
        table_name="comments",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_comments_comment_created"),
        table_name="comments",
        schema="metadata",
    )
    op.drop_table("comments", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_threads_thread_updated"),
        table_name="threads",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_threads_thread_created"),
        table_name="threads",
        schema="metadata",
    )
    op.drop_table("threads", schema="metadata")
