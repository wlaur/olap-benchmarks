"""add_query_execution_table

Revision ID: 2f5d7f0e8a21
Revises: f92b8d5a1c41
Create Date: 2026-03-19 16:20:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2f5d7f0e8a21"
down_revision: str | Sequence[str] | None = "f92b8d5a1c41"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("create sequence if not exists seq_query_execution")

    op.create_table(
        "query_execution",
        sa.Column("id", sa.Integer(), server_default=sa.text("nextval('seq_query_execution')"), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("run_step_id", sa.Integer(), nullable=True),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("start_time", sa.DateTime(), nullable=False),
        sa.Column("end_time", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_query_execution_run_start_time", "query_execution", ["run_id", "start_time"], unique=False)
    op.create_index(
        "idx_query_execution_step_start_time",
        "query_execution",
        ["run_step_id", "start_time"],
        unique=False,
    )
    op.create_index(op.f("ix_query_execution_run_id"), "query_execution", ["run_id"], unique=False)
    op.create_index(op.f("ix_query_execution_run_step_id"), "query_execution", ["run_step_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_query_execution_run_step_id"), table_name="query_execution")
    op.drop_index(op.f("ix_query_execution_run_id"), table_name="query_execution")
    op.drop_index("idx_query_execution_step_start_time", table_name="query_execution")
    op.drop_index("idx_query_execution_run_start_time", table_name="query_execution")
    op.drop_table("query_execution")
    op.execute("drop sequence if exists seq_query_execution")
