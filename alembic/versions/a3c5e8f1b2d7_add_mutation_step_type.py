"""add_mutation_step_type

Revision ID: a3c5e8f1b2d7
Revises: 7b1f3e2c9a4d
Create Date: 2026-03-17 12:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3c5e8f1b2d7"
down_revision: str | Sequence[str] | None = "7b1f3e2c9a4d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # DuckDB does not support ALTER TABLE DROP CONSTRAINT, so we recreate the table
    # to update the check constraint.
    op.execute(
        """
        CREATE TABLE run_step_new AS SELECT * FROM run_step;
        DROP TABLE run_step;
        CREATE TABLE run_step (
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
        INSERT INTO run_step SELECT * FROM run_step_new;
        DROP TABLE run_step_new;
        """
    )
    op.create_index("idx_run_step_query", "run_step", ["step_type", "query_name", "iteration"], unique=False)
    op.create_index(op.f("ix_run_step_query_name"), "run_step", ["query_name"], unique=False)
    op.create_index(op.f("ix_run_step_run_id"), "run_step", ["run_id"], unique=False)


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE run_step_new AS SELECT * FROM run_step;
        DROP TABLE run_step;
        CREATE TABLE run_step (
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
            CHECK (step_type in ('phase', 'query')),
            CHECK (status in ('running', 'completed', 'failed'))
        );
        INSERT INTO run_step SELECT * FROM run_step_new;
        DROP TABLE run_step_new;
        """
    )
    op.create_index("idx_run_step_query", "run_step", ["step_type", "query_name", "iteration"], unique=False)
    op.create_index(op.f("ix_run_step_query_name"), "run_step", ["query_name"], unique=False)
    op.create_index(op.f("ix_run_step_run_id"), "run_step", ["run_id"], unique=False)
