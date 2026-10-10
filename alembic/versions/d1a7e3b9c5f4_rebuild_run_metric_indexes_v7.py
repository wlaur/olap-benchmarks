"""rebuild_run_metric_indexes_v7

Revision ID: d1a7e3b9c5f4
Revises: c8f2a6d4e1b9
Create Date: 2026-10-10 14:05:00.000000

Recreates the indexes dropped by c8f2a6d4e1b9 so its column changes could run. This is a
separate revision because DuckDB aborts the process when an index is recreated under a
name dropped in the same transaction.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "d1a7e3b9c5f4"
down_revision: str | Sequence[str] | None = "c8f2a6d4e1b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("idx_run_metric_run_time", "run_metric", ["run_id", "time"], unique=False)
    op.create_index(op.f("ix_run_metric_run_id"), "run_metric", ["run_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_run_metric_run_id"), table_name="run_metric")
    op.drop_index("idx_run_metric_run_time", table_name="run_metric")
