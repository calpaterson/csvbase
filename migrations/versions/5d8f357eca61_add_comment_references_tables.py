"""Add comment references tables

Revision ID: 5d8f357eca61
Revises: 6c7715349588
Create Date: 2024-08-30 11:04:25.034919+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5d8f357eca61"
down_revision = "6c7715349588"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "comment_references",
        sa.Column("thread_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_id", sa.BigInteger(), nullable=False),
        sa.Column("referenced_thread_id", sa.BigInteger(), nullable=False),
        sa.Column("referenced_comment_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["referenced_thread_id", "referenced_comment_id"],
            ["metadata.comments.thread_id", "metadata.comments.comment_id"],
            name=op.f("fk_comment_references_referenced_thread_id_comments"),
        ),
        sa.ForeignKeyConstraint(
            ["thread_id", "comment_id"],
            ["metadata.comments.thread_id", "metadata.comments.comment_id"],
            name=op.f("fk_comment_references_thread_id_comments"),
        ),
        sa.PrimaryKeyConstraint(
            "thread_id",
            "comment_id",
            "referenced_thread_id",
            "referenced_comment_id",
            name=op.f("pk_comment_references"),
        ),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comment_references_comment_id"),
        "comment_references",
        ["comment_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comment_references_referenced_comment_id"),
        "comment_references",
        ["referenced_comment_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comment_references_referenced_thread_id"),
        "comment_references",
        ["referenced_thread_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_comment_references_thread_id"),
        "comment_references",
        ["thread_id"],
        unique=False,
        schema="metadata",
    )
    op.create_table(
        "row_references",
        sa.Column("thread_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_id", sa.BigInteger(), nullable=False),
        sa.Column("referenced_table_uuid", sa.UUID(), nullable=False),
        sa.Column("referenced_csvbase_row_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["referenced_table_uuid"],
            ["metadata.tables.table_uuid"],
            name=op.f("fk_row_references_referenced_table_uuid_tables"),
        ),
        sa.ForeignKeyConstraint(
            ["thread_id", "comment_id"],
            ["metadata.comments.thread_id", "metadata.comments.comment_id"],
            name=op.f("fk_row_references_thread_id_comments"),
        ),
        sa.PrimaryKeyConstraint(
            "thread_id",
            "comment_id",
            "referenced_table_uuid",
            "referenced_csvbase_row_id",
            name=op.f("pk_row_references"),
        ),
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_row_references_comment_id"),
        "row_references",
        ["comment_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_row_references_referenced_csvbase_row_id"),
        "row_references",
        ["referenced_csvbase_row_id"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_row_references_referenced_table_uuid"),
        "row_references",
        ["referenced_table_uuid"],
        unique=False,
        schema="metadata",
    )
    op.create_index(
        op.f("ix_metadata_row_references_thread_id"),
        "row_references",
        ["thread_id"],
        unique=False,
        schema="metadata",
    )


def downgrade():
    op.drop_index(
        op.f("ix_metadata_row_references_thread_id"),
        table_name="row_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_row_references_referenced_table_uuid"),
        table_name="row_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_row_references_referenced_csvbase_row_id"),
        table_name="row_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_row_references_comment_id"),
        table_name="row_references",
        schema="metadata",
    )
    op.drop_table("row_references", schema="metadata")
    op.drop_index(
        op.f("ix_metadata_comment_references_thread_id"),
        table_name="comment_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_comment_references_referenced_thread_id"),
        table_name="comment_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_comment_references_referenced_comment_id"),
        table_name="comment_references",
        schema="metadata",
    )
    op.drop_index(
        op.f("ix_metadata_comment_references_comment_id"),
        table_name="comment_references",
        schema="metadata",
    )
    op.drop_table("comment_references", schema="metadata")
    # ### end Alembic commands ###
