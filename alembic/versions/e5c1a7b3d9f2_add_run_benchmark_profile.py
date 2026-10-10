"""add_run_benchmark_profile

Revision ID: e5c1a7b3d9f2
Revises: d1a7e3b9c5f4
Create Date: 2026-10-10 18:00:00.000000

Every run records the benchmark profile it followed. Runs recorded before profiles existed used
per-suite iteration counts with iteration 1 unscored, and get the profile id `legacy-per-suite`.
Iteration roles become `warmup` and `measured` and only exist on select query steps: `first_run`
becomes `warmup`, `warm` becomes `measured`, and the roles recorded on mutate and concurrent steps
are cleared because no profile defines them.

DuckDB refuses to alter a table while an index depends on it and aborts the process when an
index is recreated under a name dropped in the same transaction, so f6d2b8c4e0a3 recreates the
run indexes.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "e5c1a7b3d9f2"
down_revision: str | Sequence[str] | None = "d1a7e3b9c5f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LEGACY_PROFILE_ID = "legacy-per-suite"
RUN_INDEXES = (
    "idx_run_status_started_at",
    "idx_run_suite_db_driver_operation",
    "ix_run_system_snapshot_id",
    "uq_run_natural_key_driver",
)


def upgrade() -> None:
    for index_name in RUN_INDEXES:
        op.execute(f"drop index {index_name}")
    op.execute(f"alter table run add column benchmark_profile varchar default '{LEGACY_PROFILE_ID}'")
    op.execute("alter table run alter column benchmark_profile set not null")
    op.execute("alter table run alter column benchmark_profile drop default")
    op.execute(
        """
        update run_step
        set iteration_role = case
            when run_id not in (select id from run where operation = 'select') or step_type <> 'query' then null
            when iteration_role = 'first_run' then 'warmup'
            else 'measured'
        end
        where iteration_role is not null
        """
    )


def downgrade() -> None:
    op.execute(
        """
        update run_step
        set iteration_role = case when iteration = 1 then 'first_run' else 'warm' end
        where step_type in ('query', 'mutation') and iteration is not null
        """
    )
    op.execute("alter table run drop column benchmark_profile")
