"""Adjust comment tables

Revision ID: 57b35f243fb0
Revises: eb87fcc5d860
Create Date: 2024-08-24 12:13:52.277857+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "57b35f243fb0"
down_revision = "eb87fcc5d860"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "threads",
        sa.Column("thread_slug", sa.String(), nullable=False),
        schema="metadata",
    )
    op.create_unique_constraint(
        op.f("uq_threads_thread_slug"), "threads", ["thread_slug"], schema="metadata"
    )
    op.execute("ALTER TABLE metadata.comments ALTER COLUMN comment_id DROP IDENTITY")
    op.drop_constraint("pk_comments", "comments", schema="metadata")
    op.create_primary_key(
        op.f("pk_comments"), "comments", ["thread_id", "comment_id"], schema="metadata"
    )


def downgrade():
    op.drop_constraint(
        op.f("uq_threads_thread_slug"), "threads", schema="metadata", type_="unique"
    )
    op.drop_column("threads", "thread_slug", schema="metadata")
    op.alter_column(
        "comments",
        "comment_id",
        existing_type=sa.BIGINT(),
        server_default=sa.Identity(  # type: ignore
            always=False,
            start=1,
            increment=1,
            minvalue=1,
            maxvalue=9223372036854775807,
            cycle=False,
            cache=1,
        ),
        existing_nullable=False,
        schema="metadata",
    )
    op.drop_constraint("pk_comments", "comments", schema="metadata")
    op.create_primary_key(
        op.f("pk_comments"), "comments", ["comment_id"], schema="metadata"
    )
