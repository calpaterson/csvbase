"""Rename created/updated fields

Revision ID: 6d59d431ee77
Revises: 57b35f243fb0
Create Date: 2024-08-27 06:40:16.671321+01:00

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "6d59d431ee77"
down_revision = "57b35f243fb0"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "threads", "thread_created", new_column_name="created", schema="metadata"
    )
    op.alter_column(
        "threads", "thread_updated", new_column_name="updated", schema="metadata"
    )
    op.alter_column(
        "comments", "comment_created", new_column_name="created", schema="metadata"
    )
    op.alter_column(
        "comments", "comment_updated", new_column_name="updated", schema="metadata"
    )
    op.drop_index(
        "ix_metadata_comments_comment_created", table_name="comments", schema="metadata"
    )
    op.drop_index(
        "ix_metadata_comments_comment_updated", table_name="comments", schema="metadata"
    )
    op.create_index(
        op.f("ix_metadata_comments_created"),
        "comments",
        ["created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comments_updated"),
        "comments",
        ["updated"],
        unique=False,
        schema="metadata",
    )
    op.drop_index(
        "ix_metadata_threads_thread_created", table_name="threads", schema="metadata"
    )
    op.drop_index(
        "ix_metadata_threads_thread_updated", table_name="threads", schema="metadata"
    )
    op.create_index(
        op.f("ix_metadata_threads_created"),
        "threads",
        ["created"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_threads_updated"),
        "threads",
        ["updated"],
        unique=False,
        schema="metadata",
    )


def downgrade():
    op.alter_column(
        "threads", "created", new_column_name="thread_created", schema="metadata"
    )
    op.alter_column(
        "threads", "updated", new_column_name="thread_updated", schema="metadata"
    )
    op.alter_column(
        "comments", "created", new_column_name="comment_created", schema="metadata"
    )
    op.alter_column(
        "comments", "updated", new_column_name="comment_updated", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_threads_updated"), table_name="threads", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_threads_created"), table_name="threads", schema="metadata"
    )
    op.create_index(
        "ix_metadata_threads_thread_updated",
        "threads",
        ["updated"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        "ix_metadata_threads_thread_created",
        "threads",
        ["created"],
        unique=False,
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_comments_updated"), table_name="comments", schema="metadata"
    )
    op.drop_index(
        op.f("ix_metadata_comments_created"), table_name="comments", schema="metadata"
    )
    op.create_index(
        "ix_metadata_comments_comment_updated",
        "comments",
        ["updated"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        "ix_metadata_comments_comment_created",
        "comments",
        ["created"],
        unique=False,
        schema="metadata",
    )
