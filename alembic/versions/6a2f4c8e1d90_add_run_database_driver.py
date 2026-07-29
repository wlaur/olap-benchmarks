"""add_run_database_driver

Revision ID: 6a2f4c8e1d90
Revises: e3b7a1c9d4f2
Create Date: 2026-07-29 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "6a2f4c8e1d90"
down_revision: str | Sequence[str] | None = "e3b7a1c9d4f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUN_NATURAL_KEY_EXPRESSIONS = [
    "system",
    "db",
    "db_version",
    "coalesce(db_driver, '')",
    "suite",
    "suite_scale_factor",
    "operation",
    "started_at",
]


def upgrade() -> None:
    op.add_column("run", sa.Column("db_driver", sa.String(), nullable=True))
    op.execute(f"create unique index uq_run_natural_key_driver on run ({', '.join(RUN_NATURAL_KEY_EXPRESSIONS)})")
    op.execute(
        "create index idx_run_suite_db_driver_operation on run (suite, suite_scale_factor, db, db_driver, operation)"
    )


def downgrade() -> None:
    op.execute("drop index uq_run_natural_key_driver")
    op.execute("drop index idx_run_suite_db_driver_operation")
    op.drop_column("run", "db_driver")
