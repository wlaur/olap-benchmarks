"""rebuild_run_indexes_after_profile

Revision ID: f6d2b8c4e0a3
Revises: e5c1a7b3d9f2
Create Date: 2026-10-10 18:05:00.000000

Recreates the run indexes dropped by e5c1a7b3d9f2 so its column change could run. This is a
separate revision because DuckDB aborts the process when an index is recreated under a name
dropped in the same transaction.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "f6d2b8c4e0a3"
down_revision: str | Sequence[str] | None = "e5c1a7b3d9f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUN_NATURAL_KEY_EXPRESSIONS = (
    "system",
    "db",
    "db_version",
    "coalesce(db_driver, '')",
    "suite",
    "suite_scale_factor",
    "operation",
    "started_at",
)


def upgrade() -> None:
    op.execute("create index idx_run_status_started_at on run (status, started_at)")
    op.execute(
        "create index idx_run_suite_db_driver_operation on run (suite, suite_scale_factor, db, db_driver, operation)"
    )
    op.execute("create index ix_run_system_snapshot_id on run (system_snapshot_id)")
    op.execute(f"create unique index uq_run_natural_key_driver on run ({', '.join(RUN_NATURAL_KEY_EXPRESSIONS)})")


def downgrade() -> None:
    op.execute("drop index uq_run_natural_key_driver")
    op.execute("drop index ix_run_system_snapshot_id")
    op.execute("drop index idx_run_suite_db_driver_operation")
    op.execute("drop index idx_run_status_started_at")
