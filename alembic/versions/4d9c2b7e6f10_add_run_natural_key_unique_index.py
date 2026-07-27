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


def _delete_duplicate_runs() -> None:
    bind = op.get_bind()
    duplicate_ids = [
        int(row[0])
        for row in bind.exec_driver_sql(
            """
        WITH ranked AS (
            SELECT
                id,
                row_number() OVER (
                    PARTITION BY
                        system,
                        db,
                        db_version,
                        suite,
                        suite_scale_factor,
                        operation,
                        started_at
                    ORDER BY
                        CASE status
                            WHEN 'completed' THEN 0
                            WHEN 'running' THEN 1
                            WHEN 'failed' THEN 2
                            ELSE 3
                        END,
                        finished_at DESC NULLS LAST,
                        id
                ) AS natural_key_rank
            FROM run
        )
        SELECT id
        FROM ranked
        WHERE natural_key_rank > 1
        """
        ).all()
    ]
    if not duplicate_ids:
        return

    op.drop_index("idx_run_suite_db_operation", table_name="run")
    op.drop_index("idx_run_status_started_at", table_name="run")
    op.execute(
        """
        CREATE TABLE run_deduped (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_run'),
            suite VARCHAR NOT NULL,
            suite_scale_factor INTEGER NOT NULL DEFAULT 1,
            db VARCHAR NOT NULL,
            db_version VARCHAR NOT NULL,
            operation VARCHAR NOT NULL,
            system VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            error_type VARCHAR,
            error_message TEXT,
            CHECK (suite_scale_factor >= 1),
            CHECK (status in ('running', 'completed', 'failed'))
        );
        INSERT INTO run_deduped
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY
                        system,
                        db,
                        db_version,
                        suite,
                        suite_scale_factor,
                        operation,
                        started_at
                    ORDER BY
                        CASE status
                            WHEN 'completed' THEN 0
                            WHEN 'running' THEN 1
                            WHEN 'failed' THEN 2
                            ELSE 3
                        END,
                        finished_at DESC NULLS LAST,
                        id
                ) AS natural_key_rank
            FROM run
        )
        SELECT
            id,
            suite,
            suite_scale_factor,
            db,
            db_version,
            operation,
            system,
            status,
            started_at,
            finished_at,
            error_type,
            error_message
        FROM ranked
        WHERE natural_key_rank = 1;
        DROP TABLE run;
        ALTER TABLE run_deduped RENAME TO run;
        """
    )
    op.create_index("idx_run_status_started_at", "run", ["status", "started_at"], unique=False)
    op.create_index(
        "idx_run_suite_db_operation", "run", ["suite", "suite_scale_factor", "db", "operation"], unique=False
    )


def upgrade() -> None:
    _delete_duplicate_runs()
    op.create_index("uq_run_natural_key", "run", RUN_NATURAL_KEY_COLUMNS, unique=True)


def downgrade() -> None:
    op.drop_index("uq_run_natural_key", table_name="run")
