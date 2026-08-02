"""Rename run_metric.mem_mb to server_mem_mb."""

from collections.abc import Sequence

from alembic import op

revision: str = "f4a7c2e91b60"
down_revision: str | Sequence[str] | None = "1c7e5a9d3b40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = """
    id INTEGER PRIMARY KEY DEFAULT nextval('seq_run_metric'),
    run_id INTEGER NOT NULL,
    time TIMESTAMP NOT NULL,
    cpu_percent FLOAT NOT NULL,
    {memory_column} INTEGER NOT NULL,
    client_mem_mb INTEGER,
    client_uss_mb INTEGER,
    disk_mb INTEGER NOT NULL
"""


def _rebuild_run_metric(old_name: str, new_name: str) -> None:
    # DuckDB cannot alter a table that has dependent index entries, and it aborts when a column
    # rename shares a transaction with index recreation, so run_metric is rebuilt instead.
    op.execute("drop index idx_run_metric_run_time")
    op.execute("drop index ix_run_metric_run_id")
    op.execute(
        f"""
        CREATE TABLE run_metric_rebuilt ({_COLUMNS.format(memory_column=new_name)});
        INSERT INTO run_metric_rebuilt
        SELECT id, run_id, time, cpu_percent, {old_name}, client_mem_mb, client_uss_mb, disk_mb
        FROM run_metric;
        DROP TABLE run_metric;
        ALTER TABLE run_metric_rebuilt RENAME TO run_metric;
        """
    )
    op.create_index("idx_run_metric_run_time", "run_metric", ["run_id", "time"], unique=False)
    op.create_index(op.f("ix_run_metric_run_id"), "run_metric", ["run_id"], unique=False)


def upgrade() -> None:
    _rebuild_run_metric("mem_mb", "server_mem_mb")


def downgrade() -> None:
    _rebuild_run_metric("server_mem_mb", "mem_mb")
