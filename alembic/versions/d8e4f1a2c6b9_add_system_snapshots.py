"""add_system_snapshots

Revision ID: d8e4f1a2c6b9
Revises: b7c4d9a8e6f2
Create Date: 2026-07-11 12:00:00.000000

"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, cast

import sqlalchemy as sa

from alembic import op

revision: str = "d8e4f1a2c6b9"
down_revision: str | Sequence[str] | None = "b7c4d9a8e6f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SYSTEM_METADATA_KEYS = ("host", "python", "docker", "methodology")


def _metadata_dict(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        parsed = json.loads(value)
        return cast(dict[str, Any], parsed) if isinstance(parsed, dict) else {}
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _json_value(value: dict[str, Any]) -> str | None:
    if not value:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _backfill_system_snapshots() -> None:
    bind = op.get_bind()
    snapshots: dict[tuple[str, str], int] = {}
    rows = bind.exec_driver_sql('SELECT id, system, "metadata" FROM run WHERE "metadata" IS NOT NULL').all()

    for run_id_value, system_value, metadata_value in rows:
        run_metadata = _metadata_dict(metadata_value)
        snapshot_metadata = {key: run_metadata.pop(key) for key in SYSTEM_METADATA_KEYS if key in run_metadata}
        if not snapshot_metadata:
            continue

        system = str(system_value)
        snapshot_json = cast(str, _json_value(snapshot_metadata))
        snapshot_key = (system, snapshot_json)
        snapshot_id = snapshots.get(snapshot_key)

        if snapshot_id is None:
            host_value = snapshot_metadata.get("host")
            host = host_value if isinstance(host_value, dict) else {}
            snapshot_id = int(bind.exec_driver_sql("SELECT nextval('seq_system_snapshot')").scalar_one())
            bind.exec_driver_sql(
                """
                INSERT INTO system_snapshot (
                    id, system, os, os_release, machine, processor,
                    cpu_count_logical, memory_total_mb, "metadata"
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    system,
                    host.get("os"),
                    host.get("os_release"),
                    host.get("machine"),
                    host.get("processor"),
                    host.get("cpu_count_logical"),
                    host.get("memory_total_mb"),
                    snapshot_json,
                ),
            )
            snapshots[snapshot_key] = snapshot_id

        bind.exec_driver_sql(
            'UPDATE run SET system_snapshot_id = ?, "metadata" = ? WHERE id = ?',
            (snapshot_id, _json_value(run_metadata), int(run_id_value)),
        )


def upgrade() -> None:
    op.execute("CREATE SEQUENCE IF NOT EXISTS seq_system_snapshot")
    op.create_table(
        "system_snapshot",
        sa.Column(
            "id",
            sa.Integer(),
            server_default=sa.text("nextval('seq_system_snapshot')"),
            nullable=False,
        ),
        sa.Column("system", sa.String(), nullable=False),
        sa.Column("os", sa.String(), nullable=True),
        sa.Column("os_release", sa.String(), nullable=True),
        sa.Column("machine", sa.String(), nullable=True),
        sa.Column("processor", sa.String(), nullable=True),
        sa.Column("cpu_count_logical", sa.Integer(), nullable=True),
        sa.Column("memory_total_mb", sa.Integer(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_system_snapshot_system", "system_snapshot", ["system"], unique=False)
    op.add_column("run", sa.Column("system_snapshot_id", sa.Integer(), nullable=True))
    op.create_index("ix_run_system_snapshot_id", "run", ["system_snapshot_id"], unique=False)
    _backfill_system_snapshots()


def downgrade() -> None:
    bind = op.get_bind()
    rows = bind.exec_driver_sql(
        """
        SELECT r.id, r."metadata", s."metadata"
        FROM run r
        JOIN system_snapshot s ON s.id = r.system_snapshot_id
        """
    ).all()
    for run_id_value, run_metadata_value, snapshot_metadata_value in rows:
        metadata = _metadata_dict(snapshot_metadata_value)
        metadata.update(_metadata_dict(run_metadata_value))
        bind.exec_driver_sql(
            'UPDATE run SET "metadata" = ? WHERE id = ?',
            (_json_value(metadata), int(run_id_value)),
        )

    op.drop_index("ix_run_system_snapshot_id", table_name="run")
    op.drop_column("run", "system_snapshot_id")
    op.drop_index("ix_system_snapshot_system", table_name="system_snapshot")
    op.drop_table("system_snapshot")
    op.execute("DROP SEQUENCE IF EXISTS seq_system_snapshot")
