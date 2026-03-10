from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, CheckConstraint, DateTime, Float, Index, Integer, Sequence, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


run_id_sequence = Sequence("seq_run")
run_step_id_sequence = Sequence("seq_run_step")
run_metric_id_sequence = Sequence("seq_run_metric")
debug_id_sequence = Sequence("seq_debug")


class Run(Base):
    __tablename__ = "run"

    id: Mapped[int] = mapped_column(
        Integer,
        run_id_sequence,
        server_default=run_id_sequence.next_value(),
        primary_key=True,
    )
    suite: Mapped[str] = mapped_column(String, nullable=False)
    db: Mapped[str] = mapped_column(String, nullable=False)
    db_version: Mapped[str] = mapped_column(String, nullable=False)
    operation: Mapped[str] = mapped_column(String, nullable=False)
    system: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    error_type: Mapped[str | None] = mapped_column(String)
    error_message: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("status in ('running', 'completed', 'failed', 'aborted')", name="ck_run_status"),
        Index("idx_run_suite_db_operation", "suite", "db", "operation"),
        Index("idx_run_status_started_at", "status", "started_at"),
    )


class RunStep(Base):
    __tablename__ = "run_step"

    id: Mapped[int] = mapped_column(
        Integer,
        run_step_id_sequence,
        server_default=run_step_id_sequence.next_value(),
        primary_key=True,
    )
    # DuckDB currently rejects updates to referenced rows, even when the PK is unchanged.
    # Keep this as an indexed scalar column and enforce parent/child cleanup in application code.
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    step_type: Mapped[str] = mapped_column(String, nullable=False)
    step_name: Mapped[str] = mapped_column(String, nullable=False)
    query_name: Mapped[str | None] = mapped_column(String, index=True)
    iteration: Mapped[int | None] = mapped_column(Integer)
    table_name: Mapped[str | None] = mapped_column(String)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String, nullable=False)
    row_count: Mapped[int | None] = mapped_column(Integer)
    error_type: Mapped[str | None] = mapped_column(String)
    error_message: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON)

    __table_args__ = (
        CheckConstraint("step_type in ('phase', 'query')", name="ck_run_step_type"),
        CheckConstraint("status in ('running', 'completed', 'failed', 'aborted')", name="ck_run_step_status"),
        Index("idx_run_step_query", "step_type", "query_name", "iteration"),
    )


class RunMetric(Base):
    __tablename__ = "run_metric"

    id: Mapped[int] = mapped_column(
        Integer,
        run_metric_id_sequence,
        server_default=run_metric_id_sequence.next_value(),
        primary_key=True,
    )
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    cpu_percent: Mapped[float] = mapped_column(Float, nullable=False)
    mem_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    disk_mb: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (Index("idx_run_metric_run_time", "run_id", "time"),)


class DebugEntry(Base):
    __tablename__ = "debug"

    id: Mapped[int] = mapped_column(
        Integer,
        debug_id_sequence,
        server_default=debug_id_sequence.next_value(),
        primary_key=True,
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)


class ResultsMeta(Base):
    __tablename__ = "results_meta"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String, nullable=False)


LEGACY_TABLES = ("benchmark", "event", "metric")
