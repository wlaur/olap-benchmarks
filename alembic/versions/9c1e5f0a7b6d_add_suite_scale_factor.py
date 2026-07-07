"""add_suite_scale_factor

Revision ID: 9c1e5f0a7b6d
Revises: 2f5d7f0e8a21
Create Date: 2026-07-07 15:30:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9c1e5f0a7b6d"
down_revision: str | Sequence[str] | None = "2f5d7f0e8a21"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE run_new (
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
        INSERT INTO run_new
        SELECT
            id,
            CASE
                WHEN suite IN ('tpch_sf10', 'tpch_sf50', 'tpch') THEN 'tpc_h'
                WHEN suite IN ('tpcds_sf1', 'tpcds') THEN 'tpc_ds'
                ELSE suite
            END,
            CASE
                WHEN suite = 'tpch_sf10' THEN 10
                WHEN suite = 'tpch_sf50' THEN 50
                WHEN suite = 'tpch' THEN 10
                ELSE 1
            END,
            db,
            db_version,
            operation,
            system,
            status,
            started_at,
            finished_at,
            error_type,
            error_message
        FROM run;
        DROP TABLE run;
        ALTER TABLE run_new RENAME TO run;
        """
    )
    op.create_index("idx_run_status_started_at", "run", ["status", "started_at"], unique=False)
    op.create_index(
        "idx_run_suite_db_operation", "run", ["suite", "suite_scale_factor", "db", "operation"], unique=False
    )


def downgrade() -> None:
    pass
