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
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "comments",
        "comment_id",
        existing_type=sa.BIGINT(),
        server_default=None,
        existing_nullable=False,
        schema="metadata",
    )
    op.add_column(
        "threads",
        sa.Column("thread_slug", sa.String(), nullable=False),
        schema="metadata",
    )
    op.create_unique_constraint(
        op.f("uq_threads_thread_slug"), "threads", ["thread_slug"], schema="metadata"
    )


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        op.f("uq_threads_thread_slug"), "threads", schema="metadata", type_="unique"
    )
    op.drop_column("threads", "thread_slug", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_tables_last_changed"), table_name="tables", schema="metadata"
    )
    op.drop_constraint(
        op.f("uq_stripe_subscription_statuses_stripe_subscription_status_id"),
        "stripe_subscription_statuses",
        schema="metadata",
        type_="unique",
    )
    op.alter_column(
        "comments",
        "comment_id",
        existing_type=sa.BIGINT(),
        server_default=sa.Identity(
            always=False,
            start=1,
            increment=1,
            minvalue=1,
            maxvalue=9223372036854775807,
            cycle=False,
            cache=1,
        ),
        existing_nullable=False,
        autoincrement=False,
        schema="metadata",
    )
    # ### end Alembic commands ###
