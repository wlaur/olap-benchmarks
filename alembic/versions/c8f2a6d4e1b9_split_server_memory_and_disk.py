"""split_server_memory_and_disk

Revision ID: c8f2a6d4e1b9
Revises: b2d6f90c4a13
Create Date: 2026-10-10 14:00:00.000000

Metric schema v7. server_mem_mb was the cgroup charge, page cache included, so it becomes
server_charge_mb and the cgroup v2 anon, file and shmem counters get their own columns.
disk_mb stays the whole store and gains a data/WAL split. Historical rows keep their charge
and total and leave the new columns null: the split cannot be recovered after the fact.

DuckDB refuses to alter a table while an index depends on it and aborts the process when an
index is recreated under a name dropped in the same transaction, so d1a7e3b9c5f4 recreates
the indexes.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c8f2a6d4e1b9"
down_revision: str | Sequence[str] | None = "b2d6f90c4a13"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NEW_COLUMNS = ("server_anon_mb", "server_file_mb", "server_shmem_mb", "disk_data_mb", "disk_wal_mb")


def upgrade() -> None:
    op.drop_index(op.f("ix_run_metric_run_id"), table_name="run_metric")
    op.drop_index("idx_run_metric_run_time", table_name="run_metric")
    op.execute("alter table run_metric rename column server_mem_mb to server_charge_mb")
    for column in NEW_COLUMNS:
        op.add_column("run_metric", sa.Column(column, sa.Integer(), nullable=True))


def downgrade() -> None:
    for column in NEW_COLUMNS:
        op.drop_column("run_metric", column)
    op.execute("alter table run_metric rename column server_charge_mb to server_mem_mb")
    op.create_index("idx_run_metric_run_time", "run_metric", ["run_id", "time"], unique=False)
    op.create_index(op.f("ix_run_metric_run_id"), "run_metric", ["run_id"], unique=False)
