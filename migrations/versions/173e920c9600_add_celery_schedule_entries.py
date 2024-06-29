"""Add celery schedule entries

Revision ID: 173e920c9600
Revises: 9ad42a1ac714
Create Date: 2024-06-29 12:22:57.392839+01:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "173e920c9600"
down_revision = "9ad42a1ac714"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "schedule_entries",
        sa.Column("celery_app_name", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pickled_schedule_entry", sa.PickleType(), nullable=False),
        sa.PrimaryKeyConstraint(
            "celery_app_name", "name", name=op.f("pk_schedule_entries")
        ),
        schema="celery",
    )


def downgrade():
    op.drop_table("schedule_entries", schema="celery")
