"""remove_superseded_run_indexes

Revision ID: 1c7e5a9d3b40
Revises: 6a2f4c8e1d90
Create Date: 2026-07-29 00:00:01.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "1c7e5a9d3b40"
down_revision: str | Sequence[str] | None = "6a2f4c8e1d90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("drop index uq_run_natural_key")
    op.execute("drop index idx_run_suite_db_operation")


def downgrade() -> None:
    op.execute(
        "create unique index uq_run_natural_key "
        "on run (system, db, db_version, suite, suite_scale_factor, operation, started_at)"
    )
    op.execute("create index idx_run_suite_db_operation on run (suite, suite_scale_factor, db, operation)")
