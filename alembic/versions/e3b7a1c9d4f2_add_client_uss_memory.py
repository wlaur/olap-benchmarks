"""Add benchmark client USS memory."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e3b7a1c9d4f2"
down_revision: str | Sequence[str] | None = "c6a91f2d4e87"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("run_metric", sa.Column("client_uss_mb", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("run_metric", "client_uss_mb")
