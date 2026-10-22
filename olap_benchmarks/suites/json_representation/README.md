# JSON Representation

`json_representation` compares raw JSON text with extracted fields on the same
JSONBench SF10 events. The registry and matrix include DuckDB and ClickHouse,
with `populate` and `select` operations. Each engine stores both representations
and plans ten queries: five `raw_` queries and five matching `prepared_` queries.
These tracks are published as separate comparison families, both currently
with zero weight in the overall comparison score.

The prepared track materializes fields, not aggregates or query answers.
Population records input repair, raw insertion, and field materialization as
separate phases. Both tables share one store, so its disk total covers both
tracks; it does not attribute storage to either track individually.

## Source and representation contracts

The source is the same ten `file_0001.json.gz` through `file_0010.json.gz` files
used by [JSONBench](../jsonbench/config.py), under
`input_data_directory/jsonbench_sf10/`. They are downloaded from ClickHouse's
[public Bluesky dataset](https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_0001.json.gz).
The workload is based on [ClickHouse JSONBench](https://github.com/ClickHouse/JSONBench)
and reuses the repository's canonical five query families and input repair.
Only SF10 is accepted; the expected row count is 10,000,000 in each table.

The existing `iter_jsonbench_input_lines` path removes escaped `\u0000`, repairs
split fragments by joining them with an escaped newline, and emits an empty
object after each repaired pair to retain the input row count. Those `{}` rows
are retained in both representations. An unrepaired final fragment fails.
Temporary repaired NDJSON is removed after raw insertion. Raw storage contains
JSON text after this repair and the engine's ingestion serialization; it is not
a byte-for-byte copy of the compressed source.

| Table | Stored fields |
| --- | --- |
| `json_raw` | Required JSON text `j` |
| `json_prepared` | Nullable `kind`, `operation`, `collection`, `did`, `time_us_text`, and signed integer `time_us` |

`operation` and `collection` come from the nested `commit` object. Every raw row
produces one prepared row, including empty objects and non-commit events. The
typed `time_us` is populated only for create commits in the post, repost, and
like collections; unrelated invalid timestamp text is not eagerly converted.
The original text timestamp remains available for canonical string extrema.
No filtering, grouping, deduplication, answer precomputation, or persistent
query-specific index is part of field materialization.

DuckDB forces native storage for this suite, independently of its global
storage setting, and stores raw text in `VARCHAR`. ClickHouse uses `String` for
raw text and nullable prepared fields in two `MergeTree ORDER BY tuple()`
tables with Zstd codecs. It loads repaired NDJSON with `JSONAsString`; neither
track uses ClickHouse's native JSON type or pre-aggregated materialized views.

`representation-manifest.json` records source fingerprints and identities,
schema SQL, extraction SQL, repair semantics, storage scope, and readiness.
ClickHouse also records actual table definitions. Reuse and select reject a
missing, incomplete, or changed manifest/source/layout. Once populated, queries
read native tables and need no source files as data dependencies, but the
verification contract still checks the retained source identity and fingerprint.

## Queries and exact semantics

Each family below has a `raw_` and a `prepared_` query with matching output:

| Family | Operation |
| --- | --- |
| `01_events_by_collection` | Count all rows by collection, including missing/NULL collections |
| `02_create_events_by_collection` | Count create commits and exact distinct non-NULL users by collection |
| `03_create_events_by_hour` | Count post/repost/like create commits by collection and UTC hour |
| `04_first_post_users` | First three users by canonical first-post timestamp |
| `05_longest_post_activity` | Top three users by canonical post-activity span in milliseconds |

Families 04 and 05 preserve the canonical JSONBench order of operations:
`MIN`/`MAX` compare timestamp **text** before conversion to signed integers.
For example, text `'10000'` sorts before `'9000'`; replacing the text extrema
with extrema of the numeric prepared field changes the answer. Integer
microseconds are truncated toward zero to milliseconds before constructing the
first-post timestamp or subtracting activity endpoints. First-post timestamps
and hour extraction use UTC. User-ID tie breakers and explicit NULL ordering
make limited outputs deterministic.

Missing fields and JSON nulls map to SQL NULL; empty strings remain strings.
Distinct-user counts exclude NULL, while grouping retains the NULL group.
ClickHouse uses `JSONType` checks with `JSON_VALUE`, `uniqExact`, and nullable
casts to match this contract. Its query and materialization settings include
`function_json_value_return_type_allow_nullable=1`,
`function_json_value_return_type_allow_complex=1`, `cast_keep_nullable=1`, and
`short_circuit_function_evaluation='enable'`. Short-circuit evaluation keeps
unrelated invalid timestamp text outside guarded conversions. Timestamp outputs
use the existing normalization path. The query registry contains no unsupported
queries or answer comparison exclusions for this suite.

## Reproduction and validation

From the repository root:

```sh
uv run --no-sync olap prepare json_representation --scale-factor 10
uv run --no-sync olap benchmark duckdb json_representation populate --scale-factor 10
uv run --no-sync olap benchmark duckdb json_representation select --scale-factor 10
```

Substitute `clickhouse` for the other registered path. Run engines and measured
operations serially. These commands describe reproduction, not completed
full-scale measurements.

All ten DuckDB fixture queries passed offline on populated and empty inputs,
including UTC and Europe/Helsinki sessions. They compare raw/prepared results
and schemas with canonical JSONBench SQL executed in DuckDB. Additional goldens
cover lexical timestamp extrema and NULL distinct counts. Population fixtures
check repair, retained empty rows, one-to-one materialization, phase accounting,
staging cleanup, and manifest readiness. Same-engine agreement with canonical
SQL is not an independent proof of JSONBench correctness.

ClickHouse passed all five raw/prepared pairs on populated and empty native
fixtures, compared with DuckDB results. This live query fixture uses temporary
Memory tables rather than the full MergeTree population path. A separate native
check of the shared Parquet loader also passed; JSON representation population
itself uses NDJSON. These checks do not establish a full 10-million-row native
run or independent correctness of every answer. Full-scale native population,
answer comparison, and performance measurements remain pending.

```sh
uv run --no-sync ruff format --check olap_benchmarks/suites/json_representation olap_benchmarks/tests/test_json_representation*.py
uv run --no-sync ruff check olap_benchmarks/suites/json_representation olap_benchmarks/tests/test_json_representation*.py
uv run --no-sync pyright olap_benchmarks/suites/json_representation olap_benchmarks/tests/test_json_representation*.py
uv run --no-sync pytest -q olap_benchmarks/tests/test_json_representation*.py
```

The ClickHouse fixture is marked `live`, excluded from default pytest runs, and
requires an already available server. It creates and drops a uniquely named
temporary database and records no benchmark timings:

```sh
uv run --no-sync pytest -m live -q olap_benchmarks/tests/test_json_representation_clickhouse.py
```

Disk accounting includes the combined native raw/prepared store. Retained input
archives, temporary repair files, and spill are outside that total; store size
is not a measurement of peak temporary disk use or per-track storage.
