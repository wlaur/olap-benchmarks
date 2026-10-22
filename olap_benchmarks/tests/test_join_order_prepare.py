from __future__ import annotations

import io
import json
import tarfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import NoReturn, cast

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ..suites.join_order import prepare
from ..suites.join_order.prepare import SnapshotPin, convert_table, fingerprint, prepare_snapshot, verify_prepared_data
from ..suites.join_order.schema import JOB_SCHEMA, JOB_TABLES, arrow_schema


def minimal_csv(table: str) -> bytes:
    return (",".join("1" if column.sql_type == "integer" else "x" for column in JOB_SCHEMA[table]) + "\n").encode()


def write_archive(path: Path, entries: list[tuple[str, bytes, bytes]]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, contents, kind in entries:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.size = len(contents)
            if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                member.linkname = "/tmp/job-unsafe-target"
            archive.addfile(member, io.BytesIO(contents))


@pytest.fixture
def snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, SnapshotPin]:
    monkeypatch.setattr(prepare, "PREPARE_FREE_SPACE_BYTES", 0)
    path = tmp_path / "original.tgz"
    write_archive(path, [(f"{table}.csv", minimal_csv(table), tarfile.REGTYPE) for table in JOB_TABLES])
    digest = fingerprint(path)
    return path, SnapshotPin(digest["sha256"], bytes=digest["bytes"])


def test_default_preparation_uses_frozen_acquisition_pin_and_optional_local_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Path, SnapshotPin, Path | None]] = []

    def record_snapshot(output: Path, pin: SnapshotPin, *, archive: Path | None = None) -> None:
        calls.append((output, pin, archive))

    monkeypatch.setattr(prepare.get_settings(), "input_data_directory", tmp_path)
    monkeypatch.setattr(prepare, "prepare_snapshot", record_snapshot)
    prepare.prepare_data()
    local = tmp_path / "imdb.tgz"
    prepare.prepare_data(archive=local)
    assert calls == [
        (tmp_path / "join_order", prepare.SNAPSHOT_PIN, None),
        (tmp_path / "join_order", prepare.SNAPSHOT_PIN, local),
    ]


@pytest.mark.parametrize("digest", ["", "a" * 63, "A" * 64, "../snapshot", "g" * 64])
def test_snapshot_pin_rejects_invalid_hash(digest: str) -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        SnapshotPin(digest)


def test_snapshot_pin_rejects_invalid_size() -> None:
    with pytest.raises(ValueError, match="positive"):
        SnapshotPin("a" * 64, bytes=0)


def test_prepare_writes_complete_typed_fingerprinted_snapshot(
    tmp_path: Path, snapshot: tuple[Path, SnapshotPin]
) -> None:
    archive, pin = snapshot
    output = tmp_path / "join_order"
    prepare_snapshot(output, pin, archive=archive)
    manifest = verify_prepared_data(output, pin)
    assert set(manifest["tables"]) == set(JOB_TABLES)
    assert manifest["snapshot"] == {"sha256": pin.sha256, "bytes": pin.bytes}
    for table in JOB_TABLES:
        parquet = pq.ParquetFile(output / f"{table}.parquet")
        assert parquet.schema_arrow.equals(arrow_schema(table))
        assert parquet.metadata.num_rows == 1
        assert manifest["tables"][table]["rows"] == 1
        assert manifest["tables"][table]["csv"]["bytes"] == len(minimal_csv(table))
    before = {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in output.iterdir()}
    prepare_snapshot(output, pin, archive=archive)
    assert before == {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in output.iterdir()}


def test_preparation_is_deterministic(tmp_path: Path, snapshot: tuple[Path, SnapshotPin]) -> None:
    archive, pin = snapshot
    prepare_snapshot(tmp_path / "first", pin, archive=archive)
    prepare_snapshot(tmp_path / "second", pin, archive=archive)
    assert verify_prepared_data(tmp_path / "first", pin) == verify_prepared_data(tmp_path / "second", pin)


@pytest.mark.parametrize("partial", ["title.parquet", "manifest.json", "unexpected.txt"])
def test_prepare_refuses_partial_dataset(tmp_path: Path, snapshot: tuple[Path, SnapshotPin], partial: str) -> None:
    archive, pin = snapshot
    output = tmp_path / "join_order"
    output.mkdir()
    (output / partial).write_bytes(b"partial")
    with pytest.raises(ValueError, match="partial"):
        prepare_snapshot(output, pin, archive=archive)
    assert (output / partial).read_bytes() == b"partial"


def test_prepare_checks_lock_before_work(tmp_path: Path, snapshot: tuple[Path, SnapshotPin]) -> None:
    archive, pin = snapshot
    lock = tmp_path / ".join_order.prepare.lock"
    lock.touch()
    with pytest.raises(FileExistsError):
        prepare_snapshot(tmp_path / "join_order", pin, archive=archive)
    assert lock.exists()


def test_prepare_refuses_low_disk_before_reading_archive(
    tmp_path: Path, snapshot: tuple[Path, SnapshotPin], monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, pin = snapshot
    monkeypatch.setattr(prepare, "PREPARE_FREE_SPACE_BYTES", 2**63)
    with pytest.raises(RuntimeError, match="free bytes"):
        prepare_snapshot(tmp_path / "join_order", pin, archive=archive)
    assert not (tmp_path / ".join_order.prepare.lock").exists()


@pytest.mark.parametrize("wrong_size", [False, True])
def test_prepare_rejects_snapshot_mismatch(
    tmp_path: Path, snapshot: tuple[Path, SnapshotPin], wrong_size: bool
) -> None:
    archive, pin = snapshot
    wrong = SnapshotPin(pin.sha256 if wrong_size else "a" * 64, bytes=pin.bytes + int(wrong_size))
    with pytest.raises(ValueError, match="pinned snapshot"):
        prepare_snapshot(tmp_path / "join_order", wrong, archive=archive)
    assert archive.is_file()
    assert not (tmp_path / "join_order").exists()
    assert not list(tmp_path.glob(".join_order*"))


def test_failed_conversion_never_publishes_partial_dataset(
    tmp_path: Path, snapshot: tuple[Path, SnapshotPin], monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, pin = snapshot

    def fail_conversion(_source: Path, destination: Path, _table: str) -> NoReturn:
        destination.write_bytes(b"incomplete")
        raise RuntimeError("conversion failed")

    monkeypatch.setattr(prepare, "convert_table", fail_conversion)
    with pytest.raises(RuntimeError, match="conversion failed"):
        prepare_snapshot(tmp_path / "join_order", pin, archive=archive)
    assert not (tmp_path / "join_order").exists()
    assert not list(tmp_path.glob(".join_order*"))
    assert archive.is_file()


@pytest.mark.parametrize("change", ["parquet", "count", "schema", "pin", "version", "archive_schema"])
def test_prepared_snapshot_rejects_mutations(tmp_path: Path, snapshot: tuple[Path, SnapshotPin], change: str) -> None:
    archive, pin = snapshot
    output = tmp_path / "join_order"
    prepare_snapshot(output, pin, archive=archive)
    path = output / prepare.MANIFEST_NAME
    manifest = verify_prepared_data(output, pin)
    if change == "parquet":
        with (output / "title.parquet").open("ab") as target:
            target.write(b"unexpected")
    elif change == "count":
        manifest["tables"]["title"]["rows"] += 1
    elif change == "schema":
        manifest["schema_sha256"] = "a" * 64
    elif change == "pin":
        manifest["snapshot"]["sha256"] = "a" * 64
    elif change == "version":
        manifest["preparation_version"] += 1
    else:
        manifest["archive_schema"] = {"bytes": 0, "sha256": "not a hash"}
    path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, pa.ArrowInvalid), match="JOB|Parquet"):
        verify_prepared_data(output, pin)


def test_prepared_snapshot_rejects_symlink_inputs(tmp_path: Path, snapshot: tuple[Path, SnapshotPin]) -> None:
    archive, pin = snapshot
    output = tmp_path / "join_order"
    prepare_snapshot(output, pin, archive=archive)
    original = output / "title.parquet"
    moved = tmp_path / "title.parquet"
    original.rename(moved)
    original.symlink_to(moved)
    with pytest.raises(ValueError, match="regular files"):
        verify_prepared_data(output, pin)


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("../title.csv", tarfile.REGTYPE),
        ("/title.csv", tarfile.REGTYPE),
        ("./title.csv", tarfile.REGTYPE),
        ("title.csv", tarfile.SYMTYPE),
        ("title.csv", tarfile.LNKTYPE),
        ("title.csv", tarfile.FIFOTYPE),
        ("subdirectory/", tarfile.DIRTYPE),
        ("not_job.csv", tarfile.REGTYPE),
        ("../schematext.sql", tarfile.REGTYPE),
        ("schematext.sql", tarfile.SYMTYPE),
        ("schematext.sql", tarfile.LNKTYPE),
    ],
)
def test_extract_rejects_unsafe_or_unexpected_members(tmp_path: Path, name: str, kind: bytes) -> None:
    archive = tmp_path / "malicious.tgz"
    write_archive(archive, [(name, b"data", kind)])
    with pytest.raises(ValueError, match="unsafe"):
        prepare.extract_snapshot(archive, tmp_path / "extracted")


def test_extract_rejects_duplicate_members(tmp_path: Path) -> None:
    archive = tmp_path / "duplicate.tgz"
    write_archive(archive, [("title.csv", b"data", tarfile.REGTYPE)] * 2)
    with pytest.raises(ValueError, match="unsafe"):
        prepare.extract_snapshot(archive, tmp_path / "extracted")


def test_extract_records_bounded_schema_without_executing_or_publishing_it(tmp_path: Path) -> None:
    archive = tmp_path / "with-schema.tgz"
    schema = (prepare.DIRECTORY / "upstream/schema.sql").read_bytes()
    entries = [(f"{table}.csv", minimal_csv(table), tarfile.REGTYPE) for table in JOB_TABLES]
    entries.insert(2, ("schematext.sql", schema, tarfile.REGTYPE))
    write_archive(archive, entries)
    output = tmp_path / "prepared"
    digest = fingerprint(archive)
    pin = SnapshotPin(digest["sha256"], digest["bytes"])
    prepare_snapshot(output, pin, archive=archive)
    manifest = verify_prepared_data(output, pin)
    archived_schema = tmp_path / "schema.sql"
    archived_schema.write_bytes(schema)
    assert manifest["archive_schema"] == fingerprint(archived_schema)
    assert not (output / "schematext.sql").exists()


@pytest.mark.parametrize("contents", [b"", b"unexpected SQL", b"x" * (prepare.MAX_SCHEMA_BYTES + 1)])
def test_extract_rejects_empty_malformed_or_oversized_schema(tmp_path: Path, contents: bytes) -> None:
    archive = tmp_path / "bad-schema.tgz"
    write_archive(archive, [("schematext.sql", contents, tarfile.REGTYPE)])
    with pytest.raises(ValueError, match="empty|schema"):
        prepare.extract_snapshot(archive, tmp_path / "extracted")


def test_extract_rejects_duplicate_schema(tmp_path: Path) -> None:
    archive = tmp_path / "duplicate-schema.tgz"
    schema = (prepare.DIRECTORY / "upstream/schema.sql").read_bytes()
    write_archive(archive, [("schematext.sql", schema, tarfile.REGTYPE)] * 2)
    with pytest.raises(ValueError, match="unsafe"):
        prepare.extract_snapshot(archive, tmp_path / "extracted")


def test_extract_rejects_missing_tables(tmp_path: Path) -> None:
    archive = tmp_path / "missing.tgz"
    write_archive(archive, [("title.csv", b"data", tarfile.REGTYPE)])
    with pytest.raises(ValueError, match="missing tables"):
        prepare.extract_snapshot(archive, tmp_path / "extracted")


def test_extract_enforces_expanded_size_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = tmp_path / "oversized.tgz"
    write_archive(archive, [("title.csv", b"12345", tarfile.REGTYPE)])
    monkeypatch.setattr(prepare, "MAX_EXTRACTED_BYTES", 4)
    with pytest.raises(ValueError, match="extraction budget"):
        prepare.extract_snapshot(archive, tmp_path / "extracted")


def test_csv_preserves_null_empty_unicode_quotes_newlines_and_duplicate_join_keys(tmp_path: Path) -> None:
    source = tmp_path / "movie_companies.csv"
    value = '"a, b \\"quoted\\" \\N\nÅ"'
    source.write_text(f'1,4,7,2,\n2,4,7,2,""\n3,4,7,2,{value}\n4,4,7,2,{value}\n')
    target = tmp_path / "movie_companies.parquet"
    manifest = convert_table(source, target, "movie_companies")
    table = pl.read_parquet(target)
    rows = cast(list[dict[str, object]], table.to_dicts())
    assert [row["note"] for row in rows] == [None, "", 'a, b "quoted" \\N\nÅ', 'a, b "quoted" \\N\nÅ']
    assert [row["movie_id"] for row in rows] == [4, 4, 4, 4]
    assert [row["company_id"] for row in rows] == [7, 7, 7, 7]
    assert table.schema["movie_id"] == pl.Int32
    assert manifest["rows"] == 4
    assert manifest["max_utf8_bytes"]["note"] == len('a, b "quoted" \\N\nÅ'.encode())


@pytest.mark.parametrize("block_bytes", [1, 2, 3, 7, 31])
def test_copy_csv_preserves_escape_rules_across_stream_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, block_bytes: int
) -> None:
    monkeypatch.setattr(prepare, "_BLOCK_BYTES", block_bytes)
    values: list[str | None] = [None, "", "NULL", "\\N", "\\n", "one\\\\two", "5' 9\"", 'a,"b"\r\nÅ\tZ', "end\\"]
    raw_values = [
        "",
        '""',
        "NULL",
        "\\N",
        '"\\n"',
        "one\\\\two",
        '"5\' 9\\""',
        '"a,\\"b\\"\r\nÅ\tZ"',
        '"end\\\\"',
    ]
    source = tmp_path / "movie_companies.csv"
    source.write_bytes("".join(f"{index},4,7,2,{value}\n" for index, value in enumerate(raw_values, 1)).encode())
    target = tmp_path / "movie_companies.parquet"
    manifest = convert_table(source, target, "movie_companies")
    assert pl.read_parquet(target)["note"].to_list() == values
    assert manifest["rows"] == len(values)


def test_converter_streams_many_batches_and_accepts_large_multiline_utf8_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(prepare, "_CSV_BLOCK_BYTES", 4 * 1024**2)
    long_text = "Å" * (1024**2 + 3) + '\n"quoted"\\tail'
    escaped = long_text.replace("\\", "\\\\").replace('"', '\\"')
    source = tmp_path / "person_info.csv"
    with source.open("w", encoding="utf-8", newline="") as output:
        output.write(f'1,7,19,"{escaped}",\n')
        for index in range(2, 500_002):
            output.write(f'{index},7,19,"small",\n')
    target = tmp_path / "person_info.parquet"
    manifest = convert_table(source, target, "person_info")
    table = pq.ParquetFile(target)
    assert table.metadata.num_rows == manifest["rows"] == 500_001
    assert table.metadata.num_row_groups > 2
    first = pl.scan_parquet(target).head(1).collect().row(0, named=True)
    assert first["info"] == long_text
    assert first["note"] is None
    assert manifest["max_utf8_bytes"]["info"] == len(long_text.encode())


def test_default_verification_rejects_an_unpinned_fixture_archive(
    tmp_path: Path, snapshot: tuple[Path, SnapshotPin]
) -> None:
    archive, pin = snapshot
    output = tmp_path / "prepared"
    prepare_snapshot(output, pin, archive=archive)
    with pytest.raises(ValueError, match="provenance"):
        verify_prepared_data(output)


def test_csv_uses_original_int32_range_and_varchar_character_limits(tmp_path: Path) -> None:
    source = tmp_path / "company_type.csv"
    text = "🦆" * 32
    source.write_text(f'-2147483648,"{text}"\n2147483647,""\n', encoding="utf-8")
    output = tmp_path / "company_type.parquet"
    manifest = convert_table(source, output, "company_type")
    assert pl.read_parquet(output).rows() == [(-2147483648, text), (2147483647, "")]
    assert manifest["max_utf8_bytes"]["kind"] == 128


@pytest.mark.parametrize(
    ("table", "contents"),
    [
        ("company_type", b"1,\n"),
        ("company_type", b'1,"' + b"a" * 33 + b'"\n'),
        ("company_type", b"2147483648,kind\n"),
        ("company_type", b"1,kind,extra\n"),
        ("company_type", b"1\n"),
        ("company_type", b"1,\xff\n"),
        ("company_type", b""),
        ("company_type", b"1,kind\n\n"),
        ("company_type", b'1,"unterminated\n'),
        ("complete_cast", b'1,"",1,1\n'),
    ],
)
def test_csv_conversion_rejects_schema_violations_and_removes_partial_output(
    tmp_path: Path, table: str, contents: bytes
) -> None:
    source = tmp_path / f"{table}.csv"
    source.write_bytes(contents)
    output = tmp_path / f"{table}.parquet"
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        convert_table(source, output, table)
    assert not output.exists()


@pytest.mark.parametrize("response_kind", ["correct", "wrong_header", "oversized", "short", "redirect"])
def test_download_is_bounded_and_verified_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, response_kind: str
) -> None:
    payload = b"trusted snapshot fixture"
    original = tmp_path / "original"
    original.write_bytes(payload)
    digest = fingerprint(original)
    pin = SnapshotPin(digest["sha256"], bytes=len(payload))

    @contextmanager
    def stream(_method: str, _url: str, **kwargs: object) -> Generator[httpx.Response]:
        assert kwargs["follow_redirects"] is False
        assert kwargs["timeout"] == 60
        contents = payload
        if response_kind == "oversized":
            contents += b"extra"
        elif response_kind == "short":
            contents = payload[:-1]
        status = 302 if response_kind == "redirect" else 200
        headers = {"content-length": str(len(payload) + 1)} if response_kind == "wrong_header" else {}
        yield httpx.Response(
            status,
            headers=headers,
            stream=httpx.ByteStream(contents),
            request=httpx.Request("GET", prepare.SNAPSHOT_URL),
        )

    monkeypatch.setattr(httpx, "stream", stream)
    output = tmp_path / "imdb.tgz"
    if response_kind == "correct":
        prepare._download_archive(output, pin)
        assert output.read_bytes() == payload
    else:
        with pytest.raises((ValueError, httpx.HTTPStatusError)):
            prepare._download_archive(output, pin)
        assert not output.exists()
    assert not output.with_suffix(".part").exists()
