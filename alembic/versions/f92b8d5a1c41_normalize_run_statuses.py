"""normalize_run_statuses

Revision ID: f92b8d5a1c41
Revises: a3c5e8f1b2d7
Create Date: 2026-03-19 10:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f92b8d5a1c41"
down_revision: str | Sequence[str] | None = "a3c5e8f1b2d7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE run_new (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_run'),
            suite VARCHAR NOT NULL,
            db VARCHAR NOT NULL,
            db_version VARCHAR NOT NULL,
            operation VARCHAR NOT NULL,
            system VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            error_type VARCHAR,
            error_message TEXT,
            CHECK (status in ('running', 'completed', 'failed'))
        );
        INSERT INTO run_new
        SELECT
            id,
            suite,
            db,
            db_version,
            operation,
            system,
            CASE
                WHEN status IN ('running', 'completed', 'failed') THEN status
                ELSE 'failed'
            END,
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
    op.create_index("idx_run_suite_db_operation", "run", ["suite", "db", "operation"], unique=False)

    op.execute(
        """
        CREATE TABLE run_step_new (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_run_step'),
            run_id INTEGER NOT NULL,
            step_type VARCHAR NOT NULL,
            step_name VARCHAR NOT NULL,
            query_name VARCHAR,
            iteration INTEGER,
            table_name VARCHAR,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status VARCHAR NOT NULL,
            row_count INTEGER,
            error_type VARCHAR,
            error_message TEXT,
            metadata JSON,
            CHECK (step_type in ('phase', 'query', 'mutation')),
            CHECK (status in ('running', 'completed', 'failed'))
        );
        INSERT INTO run_step_new
        SELECT
            id,
            run_id,
            step_type,
            step_name,
            query_name,
            iteration,
            table_name,
            started_at,
            finished_at,
            CASE
                WHEN status IN ('running', 'completed', 'failed') THEN status
                ELSE 'failed'
            END,
            row_count,
            error_type,
            error_message,
            metadata
        FROM run_step;
        DROP TABLE run_step;
        ALTER TABLE run_step_new RENAME TO run_step;
        """
    )
    op.create_index("idx_run_step_query", "run_step", ["step_type", "query_name", "iteration"], unique=False)
    op.create_index(op.f("ix_run_step_query_name"), "run_step", ["query_name"], unique=False)
    op.create_index(op.f("ix_run_step_run_id"), "run_step", ["run_id"], unique=False)


def downgrade() -> None:
    pass
