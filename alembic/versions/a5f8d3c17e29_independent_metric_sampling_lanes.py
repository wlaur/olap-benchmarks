"""independent_metric_sampling_lanes

Revision ID: a5f8d3c17e29
Revises: f4a7c2e91b60
Create Date: 2026-08-03 10:00:00.000000

Each metric family is now sampled on its own schedule and writes its own row,
leaving the columns it does not measure null.

DuckDB refuses to alter a column while an index depends on the table, and aborts the
process outright if an index is recreated under a name dropped in the same transaction.
env.py runs one transaction per migration, so the indexes are rebuilt by the follow-up
revision rather than here.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "a5f8d3c17e29"
down_revision: str | Sequence[str] | None = "f4a7c2e91b60"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NULLABLE_COLUMNS = ("cpu_percent", "server_mem_mb", "disk_mb")


def upgrade() -> None:
    op.drop_index(op.f("ix_run_metric_run_id"), table_name="run_metric")
    op.drop_index("idx_run_metric_run_time", table_name="run_metric")
    for column in NULLABLE_COLUMNS:
        op.execute(f"alter table run_metric alter column {column} drop not null")


def downgrade() -> None:
    op.execute("delete from run_metric where cpu_percent is null or server_mem_mb is null or disk_mb is null")
    for column in NULLABLE_COLUMNS:
        op.execute(f"alter table run_metric alter column {column} set not null")
    op.create_index("idx_run_metric_run_time", "run_metric", ["run_id", "time"], unique=False)
    op.create_index(op.f("ix_run_metric_run_id"), "run_metric", ["run_id"], unique=False)
