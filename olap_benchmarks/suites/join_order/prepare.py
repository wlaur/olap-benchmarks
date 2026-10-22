from __future__ import annotations

import io
import json
import logging
import re
import shutil
import tarfile
from collections.abc import Buffer
from dataclasses import asdict, dataclass
from hashlib import file_digest, sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import BinaryIO, TypedDict, cast

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import csv
from pydantic import TypeAdapter

from ...settings import get_settings
from .schema import DIRECTORY, JOB_SCHEMA, JOB_TABLES, UPSTREAM_COMMIT, arrow_schema

SNAPSHOT_URL = "https://event.cwi.nl/da/job/imdb.tgz"
ARCHIVE_BYTES = 1_263_193_115
ARCHIVE_SHA256 = "25f9d893c54f903366e0c263f88db0d429dbc2b159d4987ebc1e203242a7e988"
MAX_EXTRACTED_BYTES = 8 * 1024**3
MAX_SCHEMA_BYTES = 64 * 1024
PREPARE_FREE_SPACE_BYTES = 12 * 1024**3
MANIFEST_NAME = "manifest.json"
PREPARATION_VERSION = 2
_BLOCK_BYTES = 1024**2
_CSV_BLOCK_BYTES = 16 * 1024**2
_CSV_SPECIAL = re.compile(rb'["\\]')
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SnapshotPin:
    sha256: str
    bytes: int = ARCHIVE_BYTES

    def __post_init__(self) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", self.sha256) is None:
            raise ValueError("JOB archive SHA-256 must be 64 lowercase hexadecimal characters")
        if self.bytes < 1:
            raise ValueError("JOB archive byte length must be positive")


SNAPSHOT_PIN = SnapshotPin(ARCHIVE_SHA256)


class FileFingerprint(TypedDict):
    bytes: int
    sha256: str


ARCHIVE_SCHEMA: FileFingerprint = {
    "bytes": 4282,
    "sha256": "da8c4c81d42afd94cfce594a424ca2f66e6074bb808ad5622561aeff7d6e2c45",
}


class TableManifest(TypedDict):
    rows: int
    csv: FileFingerprint
    parquet: FileFingerprint
    max_utf8_bytes: dict[str, int]


class DatasetManifest(TypedDict):
    preparation_version: int
    upstream_commit: str
    schema_sha256: str
    source_url: str
    snapshot: FileFingerprint
    archive_schema: FileFingerprint | None
    pyarrow_version: str
    tables: dict[str, TableManifest]


_MANIFEST_ADAPTER = TypeAdapter(DatasetManifest)


def fingerprint(path: Path) -> FileFingerprint:
    before = path.stat()
    with path.open("rb") as source:
        digest = file_digest(source, "sha256").hexdigest()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"JOB input changed while hashing: {path}")
    return {"bytes": before.st_size, "sha256": digest}


def _verify_archive(archive: Path, pin: SnapshotPin) -> None:
    if archive.is_symlink() or not archive.is_file():
        raise ValueError(f"JOB archive must be a regular file: {archive}")
    if archive.stat().st_size != pin.bytes or fingerprint(archive) != asdict(pin):
        raise ValueError("JOB archive does not match the pinned snapshot SHA-256 and byte length")


def _download_archive(destination: Path, pin: SnapshotPin) -> None:
    partial = destination.with_suffix(".part")
    try:
        with httpx.stream("GET", SNAPSHOT_URL, timeout=60, follow_redirects=False) as response:
            response.raise_for_status()
            length = response.headers.get("content-length")
            if length is not None and int(length) != pin.bytes:
                raise ValueError("JOB download Content-Length does not match the pinned snapshot")
            written = 0
            with partial.open("xb") as output:
                for chunk in response.iter_raw(_BLOCK_BYTES):
                    written += len(chunk)
                    if written > pin.bytes:
                        raise ValueError("JOB download exceeds the pinned snapshot byte length")
                    output.write(chunk)
        _verify_archive(partial, pin)
        partial.rename(destination)
    finally:
        partial.unlink(missing_ok=True)


def extract_snapshot(archive: Path, destination: Path) -> FileFingerprint | None:
    expected = {f"{table}.csv" for table in JOB_TABLES}
    allowed = expected | {"schematext.sql"}
    seen: set[str] = set()
    total = 0
    archive_schema: FileFingerprint | None = None
    destination.mkdir()
    with tarfile.open(archive, mode="r|gz") as source:
        for member in source:
            path = PurePosixPath(member.name)
            if (
                not member.isfile()
                or member.issparse()
                or len(path.parts) != 1
                or member.name != path.name
                or path.name not in allowed
                or path.name in seen
            ):
                raise ValueError(f"Unexpected or unsafe JOB archive member: {member.name}")
            total += member.size
            if member.size < 1 or total > MAX_EXTRACTED_BYTES:
                raise ValueError("JOB archive exceeds the extraction budget or contains an empty table")
            contents = source.extractfile(member)
            if contents is None:
                raise ValueError(f"Unreadable JOB archive member: {member.name}")
            if path.name == "schematext.sql":
                if member.size > MAX_SCHEMA_BYTES:
                    raise ValueError("JOB archive schema exceeds its size budget")
                with contents:
                    schema_bytes = contents.read(MAX_SCHEMA_BYTES + 1)
                if len(schema_bytes) != member.size:
                    raise ValueError("Truncated JOB archive schema")
                names = re.findall(r"CREATE TABLE (\w+) \(", schema_bytes.decode("utf-8"))
                if len(names) != len(JOB_TABLES) or set(names) != set(JOB_TABLES):
                    raise ValueError("JOB archive schema does not describe the expected tables")
                archive_schema = {"bytes": len(schema_bytes), "sha256": sha256(schema_bytes).hexdigest()}
                seen.add(path.name)
                continue
            with contents, (destination / path.name).open("xb") as output:
                shutil.copyfileobj(contents, output, length=_BLOCK_BYTES)
            seen.add(path.name)
    if not expected <= seen:
        raise ValueError(f"JOB archive is missing tables: {', '.join(sorted(expected - seen))}")
    return archive_schema


class _CopyCSVStream(io.RawIOBase):
    def __init__(self, source: BinaryIO) -> None:
        super().__init__()
        self._source = source
        self._quoted = False
        self._pending = b""
        self._ready = bytearray()
        self._eof = False

    def readable(self) -> bool:
        return True

    def _normalize(self, block: bytes) -> bytes:
        data = self._pending + block
        self._pending = b""
        output: list[bytes] = []
        cursor = 0
        for match in _CSV_SPECIAL.finditer(data):
            position = match.start()
            if position < cursor:
                continue
            output.append(data[cursor:position])
            character = data[position : position + 1]
            following = data[position + 1 : position + 2]
            if self._quoted and not following and not self._eof:
                self._pending = character
                cursor = len(data)
                break
            if character == b'"':
                if self._quoted and following == b'"':
                    cursor = position + 2
                    continue
                self._quoted = not self._quoted
            elif self._quoted and following in (b'"', b"\\"):
                output.append(b'""' if following == b'"' else b"\\")
                cursor = position + 2
                continue
            output.append(character)
            cursor = position + 1
        output.append(data[cursor:])
        if self._eof and self._quoted:
            raise ValueError("Unterminated quoted JOB CSV field")
        return b"".join(output)

    def readinto(self, buffer: Buffer) -> int:
        target = memoryview(buffer).cast("B")
        if not target:
            return 0
        while not self._ready and not self._eof:
            block = self._source.read(_BLOCK_BYTES)
            self._eof = not block
            self._ready.extend(self._normalize(block))
        size = min(len(target), len(self._ready))
        target[:size] = self._ready[:size]
        del self._ready[:size]
        return size


def convert_table(source: Path, destination: Path, table: str) -> TableManifest:
    schema = arrow_schema(table)
    rows = 0
    max_bytes: dict[str, int] = {column.name: 0 for column in JOB_SCHEMA[table] if column.sql_type != "integer"}
    try:
        with (
            source.open("rb") as raw,
            io.BufferedReader(_CopyCSVStream(raw)) as normalized,
            csv.open_csv(
                normalized,
                read_options=csv.ReadOptions(column_names=schema.names, block_size=_CSV_BLOCK_BYTES, use_threads=False),
                parse_options=csv.ParseOptions(newlines_in_values=True, ignore_empty_lines=False),
                convert_options=csv.ConvertOptions(
                    column_types={name: pa.string() for name in schema.names},
                    null_values=[""],
                    strings_can_be_null=True,
                    quoted_strings_can_be_null=False,
                    check_utf8=True,
                ),
            ) as reader,
            pq.ParquetWriter(destination, schema, compression="zstd", compression_level=3, version="2.6") as writer,
        ):
            for batch in reader:
                for index, column in enumerate(JOB_SCHEMA[table]):
                    values = cast("pa.Array[pa.Scalar[pa.DataType]]", batch.column(index))
                    if not column.nullable and values.null_count:
                        raise ValueError(f"NULL violates JOB schema: {table}.{column.name}")
                    if column.sql_type == "integer":
                        continue
                    strings = cast(pa.StringArray, values)
                    if column.max_characters is not None:
                        max_characters = cast(int | None, pc.max(pc.utf8_length(strings)).as_py())
                        if max_characters is not None and max_characters > column.max_characters:
                            raise ValueError(f"Text exceeds JOB schema: {table}.{column.name}")
                    maximum = cast(int | None, pc.max(pc.binary_length(strings)).as_py())
                    max_bytes[column.name] = max(max_bytes[column.name], maximum or 0)
                writer.write_batch(batch.cast(schema), row_group_size=65_536)
                rows += batch.num_rows
        if rows == 0:
            raise ValueError(f"JOB table is empty: {table}")
        return {
            "rows": rows,
            "csv": fingerprint(source),
            "parquet": fingerprint(destination),
            "max_utf8_bytes": max_bytes,
        }
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


def verify_prepared_data(directory: Path, pin: SnapshotPin = SNAPSHOT_PIN) -> DatasetManifest:
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("JOB prepared dataset must be a regular directory")
    expected = {MANIFEST_NAME, *(f"{table}.parquet" for table in JOB_TABLES)}
    if {path.name for path in directory.iterdir()} != expected:
        raise ValueError(f"{directory} contains a partial or unexpected JOB dataset; prepare into an empty directory")
    if any(path.is_symlink() or not path.is_file() for path in directory.iterdir()):
        raise ValueError("JOB prepared inputs must be regular files")
    try:
        manifest = _MANIFEST_ADAPTER.validate_json((directory / MANIFEST_NAME).read_text(), strict=True)
        snapshot = manifest["snapshot"]
        recorded_pin = SnapshotPin(sha256=snapshot["sha256"], bytes=snapshot["bytes"])
        archive_schema = manifest["archive_schema"]
        if archive_schema is not None:
            SnapshotPin(sha256=archive_schema["sha256"], bytes=archive_schema["bytes"])
        if (
            manifest["preparation_version"] != PREPARATION_VERSION
            or manifest["upstream_commit"] != UPSTREAM_COMMIT
            or manifest["schema_sha256"] != fingerprint(DIRECTORY / "upstream/schema.sql")["sha256"]
            or manifest["source_url"] != SNAPSHOT_URL
            or recorded_pin != pin
            or (pin == SNAPSHOT_PIN and archive_schema != ARCHIVE_SCHEMA)
            or (archive_schema is not None and archive_schema["bytes"] > MAX_SCHEMA_BYTES)
            or set(manifest["tables"]) != set(JOB_TABLES)
        ):
            raise ValueError("JOB manifest provenance does not match the requested workload")
        for table in JOB_TABLES:
            path = directory / f"{table}.parquet"
            entry = manifest["tables"][table]
            SnapshotPin(entry["csv"]["sha256"], bytes=entry["csv"]["bytes"])
            expected_text = {column.name for column in JOB_SCHEMA[table] if column.sql_type != "integer"}
            metadata = pq.ParquetFile(path).metadata
            if (
                fingerprint(path) != entry["parquet"]
                or metadata.num_rows != entry["rows"]
                or entry["rows"] < 1
                or set(entry["max_utf8_bytes"]) != expected_text
                or any(size < 0 for size in entry["max_utf8_bytes"].values())
                or not metadata.schema.to_arrow_schema().equals(arrow_schema(table))
            ):
                raise ValueError(f"JOB prepared table fingerprint/schema/row count mismatch: {table}")
    except (KeyError, TypeError, OverflowError) as exc:
        raise ValueError("Malformed JOB manifest") from exc
    return manifest


def prepare_snapshot(output: Path, pin: SnapshotPin, *, archive: Path | None = None) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.is_symlink() or (output.exists() and not output.is_dir()):
        raise ValueError("JOB output must be a regular directory")
    lock = output.with_name(f".{output.name}.prepare.lock")
    with lock.open("x"):
        try:
            if output.exists() and any(output.iterdir()):
                verify_prepared_data(output, pin)
                return
            if shutil.disk_usage(output.parent).free < PREPARE_FREE_SPACE_BYTES:
                raise RuntimeError(f"Refusing to prepare JOB: need {PREPARE_FREE_SPACE_BYTES:_} free bytes")
            with TemporaryDirectory(prefix=f".{output.name}-", dir=output.parent) as temporary:
                staging = Path(temporary)
                source = archive if archive is not None else staging / "imdb.tgz"
                if archive is None:
                    _download_archive(source, pin)
                _verify_archive(source, pin)
                csv_directory = staging / "csv"
                archive_schema = extract_snapshot(source, csv_directory)
                prepared = staging / "prepared"
                prepared.mkdir()
                tables: dict[str, TableManifest] = {}
                for table in JOB_TABLES:
                    csv_path = csv_directory / f"{table}.csv"
                    tables[table] = convert_table(csv_path, prepared / f"{table}.parquet", table)
                    _LOGGER.info("Prepared JOB %s: %s rows", table, tables[table]["rows"])
                    csv_path.unlink()
                manifest = DatasetManifest(
                    preparation_version=PREPARATION_VERSION,
                    upstream_commit=UPSTREAM_COMMIT,
                    schema_sha256=fingerprint(DIRECTORY / "upstream/schema.sql")["sha256"],
                    source_url=SNAPSHOT_URL,
                    snapshot={"sha256": pin.sha256, "bytes": pin.bytes},
                    archive_schema=archive_schema,
                    pyarrow_version=pa.__version__,
                    tables=tables,
                )
                (prepared / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
                verify_prepared_data(prepared, pin)
                prepared.rename(output)
        finally:
            lock.unlink(missing_ok=True)


def prepare_data(*, archive: Path | None = None) -> None:
    prepare_snapshot(get_settings().input_data_directory / "join_order", SNAPSHOT_PIN, archive=archive)
