"""rebuild_run_metric_indexes

Revision ID: b2d6f90c4a13
Revises: a5f8d3c17e29
Create Date: 2026-08-03 10:05:00.000000

Recreates the indexes dropped by a5f8d3c17e29 so the column alters could run. This is a
separate revision because DuckDB aborts the process when an index is recreated under a
name dropped in the same transaction.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "b2d6f90c4a13"
down_revision: str | Sequence[str] | None = "a5f8d3c17e29"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("idx_run_metric_run_time", "run_metric", ["run_id", "time"], unique=False)
    op.create_index(op.f("ix_run_metric_run_id"), "run_metric", ["run_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_run_metric_run_id"), table_name="run_metric")
    op.drop_index("idx_run_metric_run_time", table_name="run_metric")
