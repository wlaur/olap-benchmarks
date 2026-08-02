from __future__ import annotations

import io
import json
from pathlib import Path

import polars as pl
import pytest

from ..settings import REPO_ROOT
from ..suites.chat_threads.config import (
    ANCHOR_THREAD_IDS,
    ANCHOR_USER_IDS,
    CHAT_THREADS_MUTATE_STEPS,
    CHAT_THREADS_QUERIES_DIRECTORY,
    CHAT_THREADS_QUERY_NAMES,
)
from ..suites.chat_threads.corpus import CONTENT_SCHEMA, TOPICS
from ..suites.chat_threads.generator import (
    ANCHOR_USER_COUNT,
    build_chat_generation_spec,
    generate_thread,
)

# every engine that registers the suite needs a full set of query overrides, because none of
# them share DuckDB's json function names
OVERRIDE_DATABASES = ("clickhouse", "monetdb", "postgres", "timescaledb")

SAMPLE_THREAD_INDEXES = range(400, 900)


def _sample_messages(thread_indexes: range) -> list[str]:
    spec = build_chat_generation_spec(1)
    contents: list[str] = []
    for thread_index in thread_indexes:
        _thread, messages = generate_thread(spec, 400, thread_index, 0)
        contents.extend(message.content for message in messages)
    return contents


def test_generation_is_deterministic() -> None:
    spec = build_chat_generation_spec(1)
    first_thread, first_messages = generate_thread(spec, 3, 120, 0)
    second_thread, second_messages = generate_thread(spec, 3, 120, 0)

    assert first_thread == second_thread
    assert first_messages == second_messages


def test_thread_row_agrees_with_its_messages() -> None:
    spec = build_chat_generation_spec(1)

    for thread_index in (0, 41, 617, 2_400):
        thread, messages = generate_thread(spec, thread_index // 40, thread_index, 0)
        assert thread.message_count == len(messages)
        assert thread.updated_at == messages[-1].created_at
        assert thread.created_at == messages[0].created_at
        assert [message.seq for message in messages] == list(range(1, len(messages) + 1))
        assert messages[0].parent_id is None


def test_anchor_threads_carry_rich_content() -> None:
    spec = build_chat_generation_spec(1)

    for thread_index in range(4):
        _thread, messages = generate_thread(spec, 0, thread_index, 0)
        part_types = {part["type"] for message in messages for part in json.loads(message.content)["parts"]}
        assert "visualization" in part_types
        assert "tool_use" in part_types
        assert {"image", "tool_result"} & part_types


def test_query_literals_reference_existing_anchors() -> None:
    # a generator change that shifts the id space would otherwise leave every point-lookup
    # query silently returning zero rows instead of failing
    anchor_user = ANCHOR_USER_IDS[0]
    anchor_thread = ANCHOR_THREAD_IDS[0]
    spec = build_chat_generation_spec(1)
    thread, messages = generate_thread(spec, 0, 0, 0)

    assert thread.thread_id == anchor_thread
    assert thread.user_id == anchor_user
    assert all(message.user_id == anchor_user for message in messages)

    referenced = [
        path
        for path in CHAT_THREADS_QUERIES_DIRECTORY.rglob("*.sql")
        if anchor_user in path.read_text() or anchor_thread in path.read_text()
    ]
    assert referenced, "no query references the anchor user or thread"


def test_every_query_has_overrides_for_json_dialects() -> None:
    for query_name in CHAT_THREADS_QUERY_NAMES:
        for database in OVERRIDE_DATABASES:
            override = CHAT_THREADS_QUERIES_DIRECTORY / database / f"{query_name}.sql"
            assert override.is_file(), f"missing {database} override for {query_name}"


def test_schema_exists_for_every_registered_database() -> None:
    schemas = REPO_ROOT / "olap_benchmarks/suites/chat_threads/schemas"
    for database in (*OVERRIDE_DATABASES, "duckdb"):
        assert (schemas / f"{database}.sql").is_file(), f"missing schema for {database}"


def test_postgres_family_casts_aggregate_sums() -> None:
    # postgres sum(bigint) returns numeric, which the answer-hash canonicaliser routes down
    # the decimal branch instead of the integer branch, so identical values hash differently
    for database in ("postgres", "timescaledb"):
        for query_name in ("13_token_usage_by_model", "14_top_users_by_tokens", "20_branch_points"):
            sql = (CHAT_THREADS_QUERIES_DIRECTORY / database / f"{query_name}.sql").read_text()
            for line in sql.splitlines():
                if "sum(" in line and " as " in line and not line.strip().startswith("--"):
                    assert "::bigint as" in line, f"{database}/{query_name}: uncast sum: {line.strip()}"


def test_mutate_step_names_are_unique() -> None:
    names = [step.name for step in CHAT_THREADS_MUTATE_STEPS]
    assert len(names) == len(set(names))


def test_messages_are_valid_documents_of_the_declared_schema() -> None:
    for content in _sample_messages(range(400, 460)):
        document = json.loads(content)
        assert document["schema"] == CONTENT_SCHEMA
        assert document["role"] in {"user", "assistant", "tool"}
        assert document["parts"], "a message must carry at least one part"
        if document["role"] == "assistant":
            assert document["usage"]["output_tokens"] > 0


def test_topics_have_distinct_terms() -> None:
    for topic in TOPICS:
        assert len(set(topic.terms)) == len(topic.terms), f"duplicate terms in {topic.name}"
        assert len(topic.terms) >= 8, f"{topic.name} needs enough terms to sample pairs"


@pytest.mark.parametrize(
    ("label", "minimum", "maximum"),
    [("text", 3.0, 5.5)],
)
def test_generated_text_compresses_like_prose(label: str, minimum: float, maximum: float) -> None:
    # a fixed sentence pool would compress several times better than English and would turn
    # the suite into a measurement of the generator rather than of the engine
    _ = label
    texts = [content for content in _sample_messages(SAMPLE_THREAD_INDEXES) if len(content) <= 20_000]
    raw_bytes = sum(len(content.encode()) for content in texts)

    buffer = io.BytesIO()
    pl.DataFrame({"content": texts}).write_parquet(buffer, compression="zstd")
    ratio = raw_bytes / buffer.tell()

    assert minimum <= ratio <= maximum, f"zstd ratio {ratio:.2f} outside the realistic band"


def test_attachment_payloads_are_not_compressible() -> None:
    # png and pdf attachments stand in for already-compressed formats, so their base64 must
    # stay close to incompressible or the storage comparison becomes meaningless
    payloads: list[str] = []
    for content in _sample_messages(SAMPLE_THREAD_INDEXES):
        document = json.loads(content)
        for part in document["parts"]:
            if part.get("type") == "image" and part["source"]["media_type"] != "text/csv":
                payloads.append(part["source"]["data"])

    assert payloads, "expected some binary attachments in the sample"
    raw_bytes = sum(len(payload) for payload in payloads)
    buffer = io.BytesIO()
    pl.DataFrame({"payload": payloads}).write_parquet(buffer, compression="zstd")
    assert raw_bytes / buffer.tell() < 1.6


def test_query_files_avoid_recursive_descent() -> None:
    # $..data also matches the vega-lite spec's data key, which silently inflates attachment
    # byte counts and made duckdb disagree with clickhouse
    def statements(path: Path) -> str:
        return "\n".join(line for line in path.read_text().splitlines() if not line.strip().startswith("--"))

    offenders: list[Path] = [
        path for path in CHAT_THREADS_QUERIES_DIRECTORY.rglob("*.sql") if "$.." in statements(path)
    ]
    assert not offenders, f"recursive descent found in {[path.name for path in offenders]}"


def test_sacrificial_threads_are_disjoint_from_anchors() -> None:
    spec = build_chat_generation_spec(1)
    anchor_threads = ANCHOR_USER_COUNT * 40
    assert spec.sacrificial_start_index > anchor_threads
