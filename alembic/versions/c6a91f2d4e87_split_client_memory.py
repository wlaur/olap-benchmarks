"""split_client_memory

Revision ID: c6a91f2d4e87
Revises: d8e4f1a2c6b9
Create Date: 2026-07-28 13:20:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c6a91f2d4e87"
down_revision: str | Sequence[str] | None = "d8e4f1a2c6b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("run_metric", sa.Column("client_mem_mb", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("run_metric", "client_mem_mb")
