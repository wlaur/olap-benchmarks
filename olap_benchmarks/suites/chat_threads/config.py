import json
import logging
import shutil
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from random import Random
from threading import Barrier
from time import perf_counter
from typing import Any, ClassVar, Literal, cast

import polars as pl

from ...dbs import Database
from ...run_metadata import StepResultStatus
from ...settings import (
    REPO_ROOT,
    SETTINGS,
    Operation,
    SuiteName,
    TableName,
    format_suite_data_directory_name,
    resolve_suite_scale_factor,
)
from .. import BenchmarkSuite
from .generator import (
    ANCHOR_THREADS_PER_USER,
    ANCHOR_USER_COUNT,
    SACRIFICIAL_THREAD_COUNT,
    ChatGenerationSpec,
    MessageRow,
    ThreadRow,
    build_chat_generation_spec,
    deterministic_uuid,
    generate_thread,
    iter_generated_rows,
    message_uuid,
)

_LOGGER = logging.getLogger(__name__)

CHAT_THREADS_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/chat_threads/queries"

THREAD_TABLE: TableName = "chat_thread"
MESSAGE_TABLE: TableName = "chat_message"

CHAT_THREADS_QUERY_NAMES = {n.stem: 5 for n in sorted(CHAT_THREADS_QUERIES_DIRECTORY.glob("*.sql"))}

GENERATION_BATCH_MESSAGES = 25_000
MESSAGE_ROW_GROUP_ROWS = 8_000
THREAD_ROW_GROUP_ROWS = 64_000

THREAD_SCHEMA: dict[str, pl.DataType] = {
    "thread_id": pl.String(),
    "user_id": pl.String(),
    "created_at": pl.Datetime("ms"),
    "updated_at": pl.Datetime("ms"),
    "title": pl.String(),
    "message_count": pl.Int32(),
    "settings": pl.String(),
}

MESSAGE_SCHEMA: dict[str, pl.DataType] = {
    "thread_id": pl.String(),
    "message_id": pl.String(),
    "user_id": pl.String(),
    "seq": pl.Int32(),
    "parent_id": pl.String(),
    "created_at": pl.Datetime("ms"),
    "content": pl.String(),
}

MutateAction = Literal[
    "append_message",
    "touch_thread",
    "stream_update_message",
    "branch_thread",
    "delete_thread",
]

MUTATE_ITERATIONS = 3
STREAM_UPDATE_REWRITES = 20

CONCURRENT_QUERY_NAMES: tuple[str, ...] = (
    "01_thread_sidebar",
    "04_thread_fetch",
    "08_keyword_search_user",
)
CONCURRENT_READER_CLIENTS = 4
CONCURRENT_READER_ITERATIONS = 5
CONCURRENT_WRITER_BATCH_ROWS = 100
CONCURRENT_WRITER_ITERATIONS = 20
CONCURRENT_WRITER_SEED_OFFSET = 900_000


@dataclass(frozen=True)
class ChatMutateStep:
    action: MutateAction
    table: TableName
    row_count: int

    @property
    def name(self) -> str:
        return f"{self.action}_{self.row_count}"


CHAT_THREADS_MUTATE_STEPS: list[ChatMutateStep] = [
    ChatMutateStep(action="append_message", table=MESSAGE_TABLE, row_count=1),
    ChatMutateStep(action="append_message", table=MESSAGE_TABLE, row_count=100),
    ChatMutateStep(action="append_message", table=MESSAGE_TABLE, row_count=1_000),
    ChatMutateStep(action="touch_thread", table=THREAD_TABLE, row_count=1),
    ChatMutateStep(action="touch_thread", table=THREAD_TABLE, row_count=100),
    ChatMutateStep(action="stream_update_message", table=MESSAGE_TABLE, row_count=STREAM_UPDATE_REWRITES),
    ChatMutateStep(action="branch_thread", table=MESSAGE_TABLE, row_count=100),
    ChatMutateStep(action="delete_thread", table=MESSAGE_TABLE, row_count=10),
]


def get_chat_threads_input_directory(scale_factor: int) -> Path:
    return SETTINGS.input_data_directory / format_suite_data_directory_name("chat_threads", scale_factor)


def get_chat_threads_input_files(scale_factor: int) -> dict[TableName, Path]:
    directory = get_chat_threads_input_directory(scale_factor)
    return {
        THREAD_TABLE: directory / "chat_thread.parquet",
        MESSAGE_TABLE: directory / "chat_message.parquet",
    }


def _thread_frame(rows: list[ThreadRow]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "thread_id": [row.thread_id for row in rows],
            "user_id": [row.user_id for row in rows],
            "created_at": [row.created_at for row in rows],
            "updated_at": [row.updated_at for row in rows],
            "title": [row.title for row in rows],
            "message_count": [row.message_count for row in rows],
            "settings": [row.settings for row in rows],
        },
        schema=THREAD_SCHEMA,
    )


def _message_frame(rows: list[MessageRow]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "thread_id": [row.thread_id for row in rows],
            "message_id": [row.message_id for row in rows],
            "user_id": [row.user_id for row in rows],
            "seq": [row.seq for row in rows],
            "parent_id": [row.parent_id for row in rows],
            "created_at": [row.created_at for row in rows],
            "content": [row.content for row in rows],
        },
        schema=MESSAGE_SCHEMA,
    )


def write_chat_dataset(spec: ChatGenerationSpec, output_directory: Path) -> None:
    files = get_chat_threads_input_files(spec.scale_factor)
    thread_parts = output_directory / ".chat_thread_parts"
    message_parts = output_directory / ".chat_message_parts"

    for parts_directory in (thread_parts, message_parts):
        if parts_directory.exists():
            shutil.rmtree(parts_directory)
        parts_directory.mkdir(parents=True, exist_ok=False)

    pending_threads: list[ThreadRow] = []
    pending_messages: list[MessageRow] = []
    batch_number = 0
    total_messages = 0

    def flush() -> None:
        nonlocal batch_number, total_messages, pending_threads, pending_messages
        if not pending_messages:
            return

        batch_number += 1
        total_messages += len(pending_messages)
        _thread_frame(pending_threads).write_parquet(thread_parts / f"part_{batch_number:05d}.parquet")
        _message_frame(pending_messages).write_parquet(message_parts / f"part_{batch_number:05d}.parquet")
        _LOGGER.info(
            f"Wrote chat_threads partition {batch_number:_} "
            f"({len(pending_threads):_} threads, {len(pending_messages):_} messages, {total_messages:_} total)"
        )
        pending_threads = []
        pending_messages = []

    try:
        for thread, messages in iter_generated_rows(spec):
            pending_threads.append(thread)
            pending_messages.extend(messages)
            if len(pending_messages) >= GENERATION_BATCH_MESSAGES:
                flush()
        flush()

        pl.scan_parquet(str(thread_parts / "*.parquet")).sink_parquet(
            files[THREAD_TABLE], row_group_size=THREAD_ROW_GROUP_ROWS
        )
        pl.scan_parquet(str(message_parts / "*.parquet")).sink_parquet(
            files[MESSAGE_TABLE], row_group_size=MESSAGE_ROW_GROUP_ROWS
        )
        _LOGGER.info(f"Stitched chat_threads dataset from {batch_number:_} partition(s), {total_messages:_} messages")
    finally:
        for parts_directory in (thread_parts, message_parts):
            if parts_directory.exists():
                shutil.rmtree(parts_directory)


def prepare_data(scale_factor: int = 1, overwrite: bool = False) -> None:
    scale_factor = resolve_suite_scale_factor("chat_threads", scale_factor)
    output_directory = get_chat_threads_input_directory(scale_factor)
    output_directory.mkdir(parents=True, exist_ok=True)

    files = get_chat_threads_input_files(scale_factor)
    if all(fpath.is_file() for fpath in files.values()) and not overwrite:
        _LOGGER.info(f"Reusing chat_threads dataset for scale factor {scale_factor}")
        return

    write_chat_dataset(build_chat_generation_spec(scale_factor), output_directory)


class ChatThreads[DBT: Database](BenchmarkSuite[DBT]):
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select", "mutate", "concurrent")
    name: SuiteName = "chat_threads"

    _row_counts: dict[TableName, int] | None = None

    @property
    def spec(self) -> ChatGenerationSpec:
        return build_chat_generation_spec(self.scale_factor)

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        if self._row_counts is None:
            self._row_counts = {
                table_name: self.parquet_row_count(fpath)
                for table_name, fpath in get_chat_threads_input_files(self.scale_factor).items()
            }
        return self._row_counts

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("chat_threads")

        for table_name, fpath in get_chat_threads_input_files(self.scale_factor).items():
            with self.db.phase_context("insert", table_name=table_name):
                self.db.insert_parquet(fpath, table_name)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()

    def load_chat_threads_query(self, query_name: str) -> str:
        db_specific = CHAT_THREADS_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = CHAT_THREADS_QUERIES_DIRECTORY / f"{query_name}.sql"
        sql_source = db_specific if db_specific.is_file() else common
        return sql_source.read_text()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def include_query(self, query_name: str) -> bool:
        db_specific = CHAT_THREADS_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = CHAT_THREADS_QUERIES_DIRECTORY / f"{query_name}.sql"
        return db_specific.is_file() or common.is_file()

    def query_skip(self, query_name: str) -> tuple[StepResultStatus, str] | None:
        if self.include_query(query_name):
            return None
        return ("unsupported", "query file is not defined for suite/database")

    def select(self) -> None:
        t0 = perf_counter()
        failed_queries = 0

        for idx, (query_name, iterations) in enumerate(CHAT_THREADS_QUERY_NAMES.items()):
            progress_label = f"({idx + 1:_}/{len(CHAT_THREADS_QUERY_NAMES):_})"
            skip = self.query_skip(query_name)
            if skip is not None:
                result_status, reason = skip
                self.record_skipped_query_steps(query_name, iterations, result_status=result_status, reason=reason)
                continue

            def log_success(
                it: int,
                df: pl.DataFrame,
                t: float,
                *,
                query_name: str = query_name,
                progress_label: str = progress_label,
                iterations: int = iterations,
            ) -> None:
                _LOGGER.info(
                    f"Executed {query_name} {progress_label} "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * t:_.2f} ms, shape=({df.shape[0]:_}, {df.shape[1]:_})"
                )

            ok = self.execute_query_with_isolation(
                query_name=query_name,
                iterations=iterations,
                query_loader=lambda query_name=query_name: self.load_chat_threads_query(query_name),
                fetch_kwargs_factory=lambda: self.fetch_kwargs,
                progress_label=progress_label,
                log_success=log_success,
            )
            if not ok:
                failed_queries += 1

        if failed_queries:
            _LOGGER.warning(
                f"Chat-threads select completed on {self.db.name} with {failed_queries:_} failed "
                f"{'queries' if failed_queries != 1 else 'query'}"
            )

        _LOGGER.info(
            f"Executed {len(CHAT_THREADS_QUERY_NAMES):_} queries "
            f"(with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )

    def get_mutate_steps(self) -> tuple[list[ChatMutateStep], list[ChatMutateStep]]:
        enabled: list[ChatMutateStep] = []
        skipped: list[ChatMutateStep] = []

        for step in CHAT_THREADS_MUTATE_STEPS:
            if self.db.is_mutation_step_enabled(self.name, step.name):
                enabled.append(step)
            else:
                skipped.append(step)

        return enabled, skipped

    def _sacrificial_thread_index(self, offset: int) -> int:
        spec = self.spec
        return spec.sacrificial_start_index + offset % SACRIFICIAL_THREAD_COUNT

    def _generate_appended_messages(self, row_count: int, seed: int) -> pl.DataFrame:
        spec = self.spec
        rng = Random(f"append:{spec.seed}:{seed}")
        rows: list[MessageRow] = []

        for row_index in range(row_count):
            thread_index = self._sacrificial_thread_index(seed * 7 + row_index)
            _thread, messages = generate_thread(spec, ANCHOR_USER_COUNT + thread_index % 97, thread_index, 0)
            source = messages[row_index % len(messages)]
            appended_seq = len(messages) + 1 + seed * 1_000 + row_index
            rows.append(
                MessageRow(
                    thread_id=deterministic_uuid("thread", thread_index),
                    message_id=message_uuid(thread_index, appended_seq),
                    user_id=source.user_id,
                    seq=appended_seq,
                    parent_id=source.message_id,
                    created_at=source.created_at + timedelta(seconds=rng.randrange(1, 600)),
                    content=source.content,
                )
            )

        return _message_frame(rows)

    def _generate_branch_messages(self, row_count: int, seed: int) -> pl.DataFrame:
        frame = self._generate_appended_messages(row_count, seed + 500)
        return frame.with_columns(seq=pl.col("seq") + 100_000)

    def _generate_touched_threads(self, row_count: int, seed: int) -> pl.DataFrame:
        spec = self.spec
        rows: list[ThreadRow] = []

        for row_index in range(row_count):
            thread_index = self._sacrificial_thread_index(seed * 13 + row_index)
            thread, _messages = generate_thread(spec, ANCHOR_USER_COUNT + thread_index % 97, thread_index, 0)
            rows.append(
                ThreadRow(
                    thread_id=deterministic_uuid("thread", thread_index),
                    user_id=thread.user_id,
                    created_at=thread.created_at,
                    updated_at=thread.updated_at + timedelta(minutes=seed + row_index + 1),
                    title=thread.title,
                    message_count=thread.message_count + seed + 1,
                    settings=thread.settings,
                )
            )

        return _thread_frame(rows)

    def _generate_delete_keys(self, row_count: int, seed: int) -> pl.DataFrame:
        thread_ids = [
            deterministic_uuid("thread", self._sacrificial_thread_index(seed * 29 + row_index))
            for row_index in range(row_count)
        ]
        return pl.DataFrame({"thread_id": thread_ids}, schema={"thread_id": pl.String()})

    def _apply_stream_update(self, seed: int) -> None:
        # a streaming assistant turn is rewritten as tokens arrive, so every rewrite must be a
        # valid document with a longer text part, not a truncated serialisation
        spec = self.spec
        thread_index = self._sacrificial_thread_index(seed * 31)
        _thread, messages = generate_thread(spec, ANCHOR_USER_COUNT + thread_index % 97, thread_index, 0)
        source = messages[-1]
        document = cast(dict[str, Any], json.loads(source.content))
        parts = cast(list[dict[str, Any]], document["parts"])
        text_index = next((idx for idx, part in enumerate(parts) if part.get("type") == "text"), 0)
        full_text = str(parts[text_index].get("text", ""))

        for rewrite in range(1, STREAM_UPDATE_REWRITES + 1):
            fraction = rewrite / STREAM_UPDATE_REWRITES
            parts[text_index]["text"] = full_text[: max(1, int(len(full_text) * fraction))]
            grown = MessageRow(
                thread_id=source.thread_id,
                message_id=source.message_id,
                user_id=source.user_id,
                seq=source.seq,
                parent_id=source.parent_id,
                created_at=source.created_at,
                content=json.dumps(document, separators=(",", ":"), ensure_ascii=False),
            )
            self.db.upsert(_message_frame([grown]), MESSAGE_TABLE, primary_key="message_id")

    def _apply_step(self, step: ChatMutateStep, seed: int) -> None:
        match step.action:
            case "append_message":
                self.db.insert(self._generate_appended_messages(step.row_count, seed), MESSAGE_TABLE)
            case "branch_thread":
                self.db.insert(self._generate_branch_messages(step.row_count, seed), MESSAGE_TABLE)
            case "touch_thread":
                self.db.upsert(self._generate_touched_threads(step.row_count, seed), THREAD_TABLE, "thread_id")
            case "stream_update_message":
                self._apply_stream_update(seed)
            case "delete_thread":
                self.db.delete(MESSAGE_TABLE, "thread_id", self._generate_delete_keys(step.row_count, seed))

    def mutate(self) -> None:
        t0 = perf_counter()
        steps, skipped_steps = self.get_mutate_steps()
        failed_steps = 0

        if skipped_steps:
            skipped_names = ", ".join(step.name for step in skipped_steps)
            _LOGGER.info(f"Skipping {len(skipped_steps):_} mutation steps for {self.db.name}: {skipped_names}")
            for step in skipped_steps:
                for iteration in range(1, MUTATE_ITERATIONS + 1):
                    self.db.record_skipped_mutation_step(
                        query_name=step.name,
                        iteration=iteration,
                        table_name=step.table,
                        reason=f"{self.db.name} disables this mutation step",
                    )

        if not steps:
            _LOGGER.info(f"No mutation steps enabled for {self.name} on {self.db.name}")
            return

        for step_idx, step in enumerate(steps):
            failed_iteration: int | None = None
            try:
                for iteration in range(1, MUTATE_ITERATIONS + 1):
                    failed_iteration = iteration
                    seed = step_idx * 1_000 + iteration

                    with self.db.mutation_context(
                        query_name=step.name,
                        iteration=iteration,
                        table_name=step.table,
                    ):
                        self._apply_step(step, seed)

                    _LOGGER.info(
                        f"Executed {step.name} ({step_idx + 1:_}/{len(steps):_}) "
                        f"iteration {iteration:_}/{MUTATE_ITERATIONS:_}"
                    )
                    failed_iteration = None
            except Exception as exc:
                self.db.rollback()
                failed_steps += 1
                start_iteration = 1 if failed_iteration is None else failed_iteration + 1
                for iteration in range(start_iteration, MUTATE_ITERATIONS + 1):
                    self.db.record_skipped_mutation_step(
                        query_name=step.name,
                        iteration=iteration,
                        table_name=step.table,
                        reason=f"mutation aborted after {type(exc).__name__}: {exc}",
                    )
                _LOGGER.exception(
                    f"Failed {step.name} ({step_idx + 1:_}/{len(steps):_}) on {self.db.name}; "
                    "continuing with remaining mutation steps"
                )

        if failed_steps:
            _LOGGER.warning(
                f"Chat-threads mutate completed on {self.db.name} with {failed_steps:_} failed "
                f"{'steps' if failed_steps != 1 else 'step'}"
            )

        _LOGGER.info(f"Executed {len(steps):_} mutation steps (with repetitions) in {perf_counter() - t0:_.2f} seconds")

    def _create_concurrent_worker_suite(self) -> "ChatThreads[DBT]":
        db_type = type(self.db)
        worker_db = db_type.model_construct()
        worker_db._current_suite = self.name
        worker_db._current_suite_scale_factor = self.scale_factor
        worker_db._result_storage = self.db.result_storage
        worker_db._run_id = self.db.run_id
        worker_db._last_start_command = self.db._last_start_command
        return cast(
            ChatThreads[DBT],
            type(self).model_construct(db=worker_db, name=self.name, scale_factor=self.scale_factor),
        )

    def _run_concurrent_writer(self, start_barrier: Barrier) -> None:
        suite = self._create_concurrent_worker_suite()
        step = ChatMutateStep(action="append_message", table=MESSAGE_TABLE, row_count=CONCURRENT_WRITER_BATCH_ROWS)
        step_name = f"concurrent_{step.name}"
        start_barrier.wait()

        try:
            if not suite.db.is_mutation_step_enabled(self.name, step.name):
                _LOGGER.info(f"Skipping concurrent writer step for {suite.db.name}: {step.name} is disabled")
                for iteration in range(1, CONCURRENT_WRITER_ITERATIONS + 1):
                    suite.db.record_skipped_mutation_step(
                        query_name=step_name,
                        iteration=iteration,
                        table_name=step.table,
                        reason=f"{suite.db.name} disables {step.name}",
                    )
                return

            for iteration in range(1, CONCURRENT_WRITER_ITERATIONS + 1):
                seed = CONCURRENT_WRITER_SEED_OFFSET + iteration
                with suite.db.mutation_context(
                    query_name=step_name,
                    iteration=iteration,
                    table_name=step.table,
                ):
                    suite._apply_step(step, seed)

                _LOGGER.info(f"Executed {step_name} iteration {iteration:_}/{CONCURRENT_WRITER_ITERATIONS:_}")
        finally:
            suite.db.close_connection()

    def _run_concurrent_reader(self, reader_id: int, start_barrier: Barrier) -> None:
        suite = self._create_concurrent_worker_suite()
        start_barrier.wait()

        try:
            for iteration in range(1, CONCURRENT_READER_ITERATIONS + 1):
                for query_name in CONCURRENT_QUERY_NAMES:
                    skip = suite.query_skip(query_name)
                    if skip is not None:
                        result_status, reason = skip
                        suite.record_skipped_query_steps(
                            query_name,
                            iteration,
                            result_status=result_status,
                            reason=reason,
                            start_iteration=iteration,
                        )
                        continue

                    with suite.db.query_context(query_name):
                        query = suite.load_chat_threads_query(query_name)
                        df, duration_seconds = suite.db.execute_query_iteration(
                            query_name=query_name,
                            iteration=iteration,
                            query=query,
                            fetch_kwargs=suite.fetch_kwargs,
                        )

                    _LOGGER.info(
                        f"Executed concurrent reader {reader_id:_} query {query_name} "
                        f"iteration {iteration:_}/{CONCURRENT_READER_ITERATIONS:_} "
                        f"in {1_000 * duration_seconds:_.2f} ms, shape=({df.shape[0]:_}, {df.shape[1]:_})"
                    )
        finally:
            suite.db.close_connection()

    def concurrent(self) -> None:
        t0 = perf_counter()
        worker_count = CONCURRENT_READER_CLIENTS + 1
        start_barrier = Barrier(worker_count)

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=f"{self.db.name}-chat-threads") as pool:
            futures = [
                pool.submit(self._run_concurrent_writer, start_barrier),
                *(
                    pool.submit(self._run_concurrent_reader, reader_id, start_barrier)
                    for reader_id in range(1, CONCURRENT_READER_CLIENTS + 1)
                ),
            ]

            for future in as_completed(futures):
                future.result()

        _LOGGER.info(
            f"Executed concurrent chat-threads workload with {CONCURRENT_READER_CLIENTS:_} reader clients, "
            f"{len(CONCURRENT_QUERY_NAMES):_} queries, and {CONCURRENT_WRITER_ITERATIONS:_} writer batches "
            f"in {perf_counter() - t0:_.2f} seconds"
        )


ANCHOR_USER_IDS: tuple[str, ...] = tuple(deterministic_uuid("user", index) for index in range(ANCHOR_USER_COUNT))
ANCHOR_THREAD_IDS: tuple[str, ...] = tuple(
    deterministic_uuid("thread", index) for index in range(ANCHOR_USER_COUNT * ANCHOR_THREADS_PER_USER)
)
