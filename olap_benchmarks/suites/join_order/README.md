# Join Order Benchmark (JOB)

This suite contains the standard 113 JOB queries, in natural order `1a` through
`33c`, over the 21 original IMDb tables. It uses the fixed May 2013 snapshot,
represented by scale factor 1. There is no synthetic generator or scaled/sample
benchmark dataset.

The source is [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark)
at commit `a39603662e023e449cb2121997a5034df9e02ebf`. The 113 shared query files
directly under `queries/` and the source files in `upstream/` are byte-for-byte
copies. Engine overrides live in separate subdirectories. `provenance.json`
records the upstream files' SHA-256 values. The shared SQL retains the original predicates, join
edges, aliases, and independent `MIN` projections, including string comparisons
of ratings. No join hints, added filters, numeric casts, or query substitutions
are applied.

The original paper is [Leis et al., *How Good Are Query Optimizers, Really?*,
PVLDB 9(3), 2015](https://www.vldb.org/pvldb/vol9/p204-leis.pdf).
The dataset is the paper's [CWI IMDb archive](https://event.cwi.nl/da/job/imdb.tgz),
with [CWI's accompanying provenance and terms](https://event.cwi.nl/da/job/readme.txt).
The upstream GitHub repository has no license file at the pinned revision.
Its README explicitly warns that later IMDb exports can change the query answers.
The dataset is distributed for query optimizer experiments and database research;
its contents are never committed here.

## Reproducible preparation

CWI's official HTTPS archive was acquired on 2026-10-22, with 1,263,193,115
compressed bytes and SHA-256
`25f9d893c54f903366e0c263f88db0d429dbc2b159d4987ebc1e203242a7e988`.
This is the hash of the initial acquisition from upstream, now frozen in
`provenance.json` and the preparer for reproducibility. Upstream supplies no
independent checksum, and none is claimed. The URL and accompanying CWI readme
identify the original May 2013 data; an ETag is not used as a content digest.

Normal CLI preparation verifies the frozen hash automatically:

```sh
uv run --no-sync olap prepare join_order
```

To reuse an already downloaded archive without another network request:

```python
from pathlib import Path
from olap_benchmarks.suites.join_order.prepare import prepare_data

prepare_data(archive=Path("data/temp/job-imdb.tgz"))
```

Preparation checks archive length and SHA-256 before extraction. Downloads use
the single HTTPS CWI URL, reject redirects and excess bytes, and have a network
timeout. Tar extraction requires all 21 expected flat CSV members and permits
the archive's additional `schematext.sql` member. The auxiliary schema must be
a regular, flat, nonempty UTF-8 file of at most 64 KiB describing the 21 expected
tables. Its fingerprint is recorded; its SQL is never executed. It contains
historical varchar/nullability differences from the pinned repository schema;
the latter is the authoritative schema used by this suite. Extraction rejects
duplicate names, traversal, symlinks, hardlinks, sparse files, devices,
directories, empty members, unexpected files, and expansions above 8 GiB.
It requires 12 GiB of free disk before starting. Work takes place in a temporary
directory on the destination filesystem under an exclusive preparation lock.
Interrupted work does not publish a partial input directory. A stale lock requires investigation
and removal by the operator; it is never silently stolen.

The archive uses PostgreSQL COPY CSV with a double-quote quote character and
backslash escape character. This differs from the default doubled-quote escaping
shown in the repository's general export instructions. A bounded streaming
normalizer converts escaped quotes and backslashes inside quoted fields to
Arrow's CSV representation. Other backslashes, including unquoted `\N`, remain
literal. The Arrow reader uses 16 MiB blocks, explicit string parsing followed
by safe signed Int32 casts, strict UTF-8, and the pinned schema's column order,
nullability and varchar character limits. Unquoted empty fields remain NULL;
quoted empty strings remain strings, and quoted empty integer fields fail.
Literal `NULL`, embedded commas, quotes, CR/LF, tabs and Unicode are preserved.
Duplicate non-key values and join fanout are preserved. The converter rejects
schema errors and blank records instead of skipping rows or coercing them to
NULL. Text exceeding Arrow's bounded record capacity fails without truncation.

The prepared `input_data_directory/join_order/` contains exactly 21 Parquet files
and `manifest.json`. The deterministic manifest contains source URL, archive
SHA-256/size, auxiliary archive schema hash/size, source commit and schema hash,
preparation version, Arrow version, per-table CSV/Parquet hashes and sizes, row
counts, and maximum UTF-8 byte widths.
Existing preparations are verified and reused without rewriting. Partial,
mutated, symlinked, or incompatible preparations are rejected. The harness's
existing input fingerprinting records the manifest and Parquet hashes; no
parallel fingerprint system is needed in shared code.

Full preparation of the acquired archive completed successfully on this machine:
21 tables, 74,190,187 rows, and 1,445,912,884 bytes of Parquet with Arrow 26.0.0.
The largest UTF-8 text value is 55,657 bytes (`person_info.info`), within the
current StarRocks text mapping. All prepared fingerprints, row counts, types,
nullability and varchar lengths passed verification. No partial preparation
directories or lock files remain. These checks validate input preparation;
engine ingestion and query correctness are separate gates below.

## Engine contracts and validation status

The registry and matrix include **DuckDB, ClickHouse, PostgreSQL, MonetDB, and
StarRocks** at SF1, with `populate` and `select` operations. All five passed the
native ingestion and query fixture gate serially: every one of the 113 queries
matched an independent SQLite reference on both populated and empty tables.
These are fixture checks, not full-snapshot benchmark measurements. Full input
preparation has completed. Final full-snapshot results require separately
recorded complete runs; PostgreSQL layout diagnostics are disclosed below.

- DuckDB: original types, nullability and primary keys in native storage, which
  was used for the native fixture gate. Parquet mode uses the shared external
  view and source-manifest path without persistent primary-key indexes.
- PostgreSQL: original types and primary keys, with `COLLATE "C"` on text and
  varchar fields to make comparisons and string minima deterministic across
  engines. The suite uses the existing Python fetch path. Population finalization
  installs the 23 pinned upstream foreign-key indexes before the existing
  all-table `ANALYZE`.
- MonetDB: original SQL schema and primary keys, loaded through the existing
  ADBC Parquet ingestion API.
- ClickHouse: original signed integer range and string nullability mapped to
  `Int32`/`String`/`Nullable`, with `MergeTree ORDER BY id`. Per-query
  `aggregate_functions_null_for_empty=1` preserves the all-NULL aggregate row
  when a join has no matches; `join_default_strictness=ALL` preserves fanout.
  The populated fixture exercises the canonical comma joins and duplicate fanout.
- StarRocks: signed `INT`, nullable strings, duplicate-key storage sorted by
  `id`, four hash buckets and one replica. Original bounded varchar lengths
  receive four UTF-8 bytes per character. Unbounded text maps to
  `VARCHAR(65533)`, and population refuses a prepared snapshot exceeding that
  byte bound. This cap is an explicit integration constraint, never truncation.

PostgreSQL alone installs the secondary foreign-key indexes from
`upstream/fkindexes.sql`; the other engines retain their existing layouts.
DuckDB's four query-15 overrides rename only the `at` table alias to `aka`,
because `AT` is reserved in DuckDB 2.0. The pinned upstream query files remain
unchanged; the complete fixture compares the adapted SQL to the original oracle.
StarRocks's `10b` and `10c` overrides quote the output alias `character`,
preserving its name and values while avoiding a reserved-word parse error.
Primary keys are retained where the native engine supports enforcement;
ClickHouse and StarRocks use their ordinary non-deduplicating storage models.
Physical differences must be disclosed with any comparison. This harness
does not replicate the paper's Linux machine, original engine versions, optimizer
experiments or physical-design variants.

The suite is registered only for these five engines. No JOB queries are marked
unsupported or excluded from answer comparison in the query registry. Matrix
inclusion describes planned coverage; it does not establish a completed run.

## Reproduction and validation

From the repository root, a full-snapshot DuckDB run uses:

```sh
uv run --no-sync olap benchmark duckdb join_order populate --scale-factor 1
uv run --no-sync olap benchmark duckdb join_order select --scale-factor 1
```

Substitute one of the other registered engines to use its schema and loader.
Run engines and measured operations serially. These commands describe how to
reproduce a run; they are not evidence that full-snapshot measurements exist.

Static tests cover all upstream hashes, the frozen acquisition pin, all 113
query contracts, original schema mappings, shared ingestion calls, preparation
integrity and corruption, bounded fetch/extraction including the auxiliary
schema, strict parsing, NULL/empty
distinction and duplicate join keys. Streaming tests exercise backslash/quote
boundaries down to one-byte input chunks, multi-batch conversion, and multiline
Unicode values exceeding 2 MiB. A prepared adversarial fixture checks every
value of all 21 tables without starting an engine.

```sh
uv run --no-sync ruff format --check olap_benchmarks/suites/join_order olap_benchmarks/tests/test_join_order*.py
uv run --no-sync ruff check olap_benchmarks/suites/join_order olap_benchmarks/tests/test_join_order*.py
uv run --no-sync pyright olap_benchmarks/suites/join_order olap_benchmarks/tests/test_join_order*.py
uv run --no-sync pytest -q olap_benchmarks/tests/test_join_order*.py
```

SQL fixture tests are marked `live` and excluded by the repository's default
pytest options. The local fixture gate checks every canonical query against an
independent standard-library SQLite reference, with explicit case-sensitive
LIKE, on empty and populated schemas. Hand-checked goldens cover
independent minima, string ratings (`'10.0'` versus `'9.0'`), multiway joins,
duplicate fanout and movie-link direction.

```sh
uv run --no-sync pytest -m live -q olap_benchmarks/tests/test_join_order_fixtures.py \
  -k 'not existing_engine'
```

The ingestion/dialect gate runs **one already provisioned empty scratch engine**
selected through `OLAP_BENCHMARKS_JOB_FIXTURE_DATABASE`. It uses
`JoinOrder.populate(restart=False)`, native shared ingestion, and native fetch
APIs, then compares all 113 queries on populated and empty tables. It records no
benchmark run or timings, starts/restarts no engine, refuses nonempty databases,
and drops its fixture tables on completion. Provision isolated database storage
first and schedule each engine separately. The selected engine must already be
available; use DuckDB native storage for this gate.

```sh
OLAP_BENCHMARKS_JOB_FIXTURE_DATABASE=duckdb uv run --no-sync pytest -m live -q \
  olap_benchmarks/tests/test_join_order_fixtures.py -k existing_engine
```

Fixture success establishes behavior on the tested inputs and storage paths. It
does not establish universal query correctness, full-snapshot completion, or
performance on the original paper's hardware and software.

## PostgreSQL upstream index layout

PostgreSQL JOB uses the `upstream-fkindexes-v1` policy: the original 21 primary
keys plus exactly the 23 statements in the pinned upstream
`upstream/fkindexes.sql`, SHA-256
`727909bbbb9b0ae2b7d5739cfcd066992fc4bf1e984b29d87bcc700c4e2583cc`.
This policy applies only to PostgreSQL `join_order`. Query SQL, hints, schemas,
data and other engines' layouts are unchanged.

Population builds the secondary indexes after ingestion and the loader's
restart, in recorded index phases, before the existing per-table
`ANALYZE` phases. SQL execution, commits and their costs remain inside the
population operation. Readback phases retain the observed catalog definitions,
columns, tables, sizes and valid/ready flags, and reject missing, invalid or
unexpected indexes. Run options disclose the intended policy, source hash and
secondary-index count; observed readiness is recorded separately as
`job_index_readiness` phase metadata. Fresh population verifies row counts
before finalization, without prematurely requiring indexes. SELECT checks all
21 full-snapshot counts and the complete index layout before any warmup.

Existing stores are checked before reuse, including when population decides
whether to skip before result recording starts. Those early checks perform no
recording or schema changes. A complete indexed store is reusable; primary-only,
partial or different layouts fail explicitly. There is no automatic migration
or index repair. Reproduce adoption using an empty PostgreSQL JOB store and a
fresh population so index construction and analysis costs are charged.
After the old PostgreSQL JOB engine has stopped, remove its old store through
the normal repository cleanup workflow or select an empty database directory.
Run these operations serially without query or table filters:

```sh
OLAP_BENCHMARKS_OPERATION_TIMEOUT_SECONDS=900 \
uv run --no-sync olap benchmark postgres join_order populate --scale-factor 1
OLAP_BENCHMARKS_OPERATION_TIMEOUT_SECONDS=900 \
OLAP_BENCHMARKS_QUERY_TIMEOUT_SECONDS=45 \
uv run --no-sync olap benchmark postgres join_order select --scale-factor 1
```

The normal SELECT profile runs every one of the 113 queries four times: one
warmup and three measured iterations. These reproduction commands do not claim
that the final population or SELECT rerun has completed.

Adoption is supported by the bounded 325.21-second diagnostic recorded in
`results/followup-tuning/postgres-job.json`. The candidate completed all 113
queries. The 112 completed PostgreSQL pairs matched exact DataFrames and DBAPI
schemas, and the table schema was unchanged. Original SELECT run 612's
45-second timeout for `29c` remains censored. Candidate `29c` completed in
0.548 seconds and matched the independent DuckDB reference's exact DataFrame
and schema; the timed-out original PostgreSQL DBAPI schema remains unproven.

Predeclared controls `1a`, `6a`, `17a` and `28a` ran in both layout directions,
with six measured samples per form. Their plans changed and used candidate
indexes. Original/candidate median seconds were 0.1138/0.0503,
0.24345/0.02686, 1.0871/4.4028 and 0.6217/0.3666 respectively: `17a`
regressed about fourfold. Across the 112 paired single fetches, the
candidate/original geometric mean was 0.4754, while their summed latency grew
from 75.61 to 107.31 seconds; `26c` regressed from 0.515 to 28.11 seconds.
These diagnostics support complete coverage and typical fixed-query gains,
not a uniform speedup or a completed family score from the partial baseline.
A balanced layout comparison must retain these regressions and the timeout.

Index construction took 23.035 seconds, followed by 2.545 seconds of analysis.
Observed database size grew from 8,293,947,071 to 10,027,595,455 bytes. All 23
candidate indexes were dropped at diagnostic completion and the primary-only
layout was verified. Dropping indexes did not restore the earlier statistics:
candidate `ANALYZE` changed them, which remains a confound in the reversed
controls. Final results require the fresh indexed population and complete
113-query SELECT rerun described above.
