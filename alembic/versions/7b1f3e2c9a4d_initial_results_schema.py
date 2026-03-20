"""initial_results_schema

Revision ID: 7b1f3e2c9a4d
Revises:
Create Date: 2026-03-10 18:55:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7b1f3e2c9a4d"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("create sequence if not exists seq_debug")
    op.execute("create sequence if not exists seq_run")
    op.execute("create sequence if not exists seq_run_metric")
    op.execute("create sequence if not exists seq_run_step")

    op.create_table(
        "debug",
        sa.Column("id", sa.Integer(), server_default=sa.text("nextval('seq_debug')"), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "run",
        sa.Column("id", sa.Integer(), server_default=sa.text("nextval('seq_run')"), nullable=False),
        sa.Column("suite", sa.String(), nullable=False),
        sa.Column("db", sa.String(), nullable=False),
        sa.Column("db_version", sa.String(), nullable=False),
        sa.Column("operation", sa.String(), nullable=False),
        sa.Column("system", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("error_type", sa.String(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.CheckConstraint("status in ('running', 'completed', 'failed')", name="ck_run_status"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_run_status_started_at", "run", ["status", "started_at"], unique=False)
    op.create_index("idx_run_suite_db_operation", "run", ["suite", "db", "operation"], unique=False)
    op.create_table(
        "run_metric",
        sa.Column("id", sa.Integer(), server_default=sa.text("nextval('seq_run_metric')"), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("time", sa.DateTime(), nullable=False),
        sa.Column("cpu_percent", sa.Float(), nullable=False),
        sa.Column("mem_mb", sa.Integer(), nullable=False),
        sa.Column("disk_mb", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_run_metric_run_time", "run_metric", ["run_id", "time"], unique=False)
    op.create_index(op.f("ix_run_metric_run_id"), "run_metric", ["run_id"], unique=False)
    op.create_table(
        "run_step",
        sa.Column("id", sa.Integer(), server_default=sa.text("nextval('seq_run_step')"), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("step_type", sa.String(), nullable=False),
        sa.Column("step_name", sa.String(), nullable=False),
        sa.Column("query_name", sa.String(), nullable=True),
        sa.Column("iteration", sa.Integer(), nullable=True),
        sa.Column("table_name", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("error_type", sa.String(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.CheckConstraint("status in ('running', 'completed', 'failed')", name="ck_run_step_status"),
        sa.CheckConstraint("step_type in ('phase', 'query')", name="ck_run_step_type"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_run_step_query", "run_step", ["step_type", "query_name", "iteration"], unique=False)
    op.create_index(op.f("ix_run_step_query_name"), "run_step", ["query_name"], unique=False)
    op.create_index(op.f("ix_run_step_run_id"), "run_step", ["run_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_run_step_run_id"), table_name="run_step")
    op.drop_index(op.f("ix_run_step_query_name"), table_name="run_step")
    op.drop_index("idx_run_step_query", table_name="run_step")
    op.drop_table("run_step")
    op.drop_index(op.f("ix_run_metric_run_id"), table_name="run_metric")
    op.drop_index("idx_run_metric_run_time", table_name="run_metric")
    op.drop_table("run_metric")
    op.drop_index("idx_run_suite_db_operation", table_name="run")
    op.drop_index("idx_run_status_started_at", table_name="run")
    op.drop_table("run")
    op.drop_table("debug")
    op.execute("drop sequence if exists seq_run_step")
    op.execute("drop sequence if exists seq_run_metric")
    op.execute("drop sequence if exists seq_run")
    op.execute("drop sequence if exists seq_debug")
