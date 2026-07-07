"""add_run_natural_key_unique_index

Revision ID: 4d9c2b7e6f10
Revises: 9c1e5f0a7b6d
Create Date: 2026-07-07 16:15:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4d9c2b7e6f10"
down_revision: str | Sequence[str] | None = "9c1e5f0a7b6d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUN_NATURAL_KEY_COLUMNS = [
    "system",
    "db",
    "db_version",
    "suite",
    "suite_scale_factor",
    "operation",
    "started_at",
]


def upgrade() -> None:
    op.create_index("uq_run_natural_key", "run", RUN_NATURAL_KEY_COLUMNS, unique=True)


def downgrade() -> None:
    op.drop_index("uq_run_natural_key", table_name="run")
