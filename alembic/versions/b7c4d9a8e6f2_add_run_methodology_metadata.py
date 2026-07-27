"""add_run_methodology_metadata

Revision ID: b7c4d9a8e6f2
Revises: 4d9c2b7e6f10
Create Date: 2026-07-08 18:30:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7c4d9a8e6f2"
down_revision: str | Sequence[str] | None = "4d9c2b7e6f10"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute('ALTER TABLE run ADD COLUMN "metadata" JSON')
    op.execute("ALTER TABLE run_step ADD COLUMN result_status VARCHAR")
    op.execute("ALTER TABLE run_step ADD COLUMN iteration_role VARCHAR")
    op.execute(
        """
        UPDATE run_step
        SET
            result_status = CASE
                WHEN step_type IN ('query', 'mutation') AND status = 'completed' THEN 'ok'
                WHEN step_type IN ('query', 'mutation') AND status = 'failed' THEN
                    CASE
                        WHEN error_type IN ('TimeoutError', 'TimeoutExpired') THEN 'timeout'
                        ELSE 'error'
                    END
                ELSE NULL
            END,
            iteration_role = CASE
                WHEN step_type IN ('query', 'mutation') AND iteration = 1 THEN 'first_run'
                WHEN step_type IN ('query', 'mutation') AND iteration > 1 THEN 'warm'
                ELSE NULL
            END
        """
    )
    op.execute("DELETE FROM run_step WHERE run_id NOT IN (SELECT id FROM run)")
    op.execute("DELETE FROM run_metric WHERE run_id NOT IN (SELECT id FROM run)")
    op.execute("DELETE FROM query_execution WHERE run_id NOT IN (SELECT id FROM run)")


def downgrade() -> None:
    op.execute('ALTER TABLE run DROP COLUMN "metadata"')
    op.execute("ALTER TABLE run_step DROP COLUMN result_status")
    op.execute("ALTER TABLE run_step DROP COLUMN iteration_role")
