# Common Parquet Analytics

`parquet_analytics` runs the existing 22 TPC-H query families against the same
eight external Parquet files at SF10. The registry and matrix include DuckDB,
Polars, and ClickHouse, with `populate` and `select` operations. Here, population
registers external relations and verifies their source contract. It does not
import the data into native tables or create a second optimized copy.

This suite measures a common file representation separately from the native
TPC-H suite. It is derived from TPC-H and is not an official TPC-H result. Its
comparison family currently has zero weight in the overall comparison score.
Registration and matrix inclusion describe intended coverage, not completed
measurements.

## Input contract and provenance

The source directory is the existing `input_data_directory/tpc_h_sf10/`, shared
with `tpc_h`. Preparation checks that it contains exactly these eight
unpartitioned Parquet files, their canonical column order and types, and their
SF10 footer row counts:

| Table      |       Rows |
| ---------- | ---------: |
| `region`   |          5 |
| `nation`   |         25 |
| `supplier` |    100,000 |
| `customer` |  1,500,000 |
| `part`     |  2,000,000 |
| `partsupp` |  8,000,000 |
| `orders`   | 15,000,000 |
| `lineitem` | 59,986,052 |

The 61 columns use the existing DuckDB TPC-H schema as their type specification:
signed integers, dates, strings, and exact `DECIMAL(15,2)` values. The
representation manifest records declared nullability, row-group sizes, and
compression codecs. Preparation rejects missing, extra, partitioned, wrongly
typed, or wrongly scaled Parquet inputs. It does not rewrite, sort, repartition,
recompress, or regenerate the files, and footer checks do not independently
verify every stored value.

Data generation belongs to the existing [TPC-H preparer](../tpc_h/config.py),
using [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) pinned to commit
`e53dea45345d3c934c724147e393983a53a40986`. The shared SQL and ClickHouse overrides
are reused from [the TPC-H query directory](../tpc_h/queries/), whose source
attribution is recorded in that preparer. Polars reuses the repository's native
TPC-H LazyFrame implementations.

## Engine and query contracts

- DuckDB forces Parquet storage mode for this suite, independently of the global
  DuckDB storage setting. The shared loader creates views over the original
  files and records their sources and view definitions in its storage manifest.
- Polars creates table links pointing to the original files and scans them as
  LazyFrames. `fetch` dispatches by canonical query name to the existing native
  TPC-H expressions; the published SQL is the semantic specification, not the
  code Polars executes. Verification rejects replaced or redirected links.
- ClickHouse mounts the original directory read-only and creates ordinary
  views over `file('parquet_analytics/<table>.parquet', 'Parquet')`. Explicit
  casts preserve the canonical types and nullability. There are no MergeTree
  imports, materialized views, or query-specific data layouts in this track.

All 22 canonical query names are planned. DuckDB uses shared SQL and ClickHouse
uses the existing TPC-H dialect overrides. Query 11 substitutes the SF10
threshold `0.00001` (`0.0001 / SF`), including in the published SQL and the Polars
execution path. The query registry contains no unsupported queries or answer
comparison exclusions for this suite.

`representation-manifest.json` records the suite, source directory, input
fingerprint, resolved source paths, sizes, modification times, and physical
layout. ClickHouse also records its actual view definitions. The manifest stays
in `building` state until population verification succeeds; reuse and select
require `ready` state and matching source/layout information. Changed sources
or layouts require rebuilding the store.

Run metadata records the physical representation and storage accounting. Disk
measurements include the owned store and required external files once; the
referenced input baseline is already included and must not be added again.
Temporary staging and spill are outside that disk total, which is not a peak
temporary-storage measurement. Reading common files does not imply identical
execution plans, caching, or client/server resource boundaries across engines.

## Reproduction and validation

From the repository root, reuse the existing TPC-H SF10 files. If they are not
present, the existing TPC-H preparer generates them using its required pinned
generator:

```sh
uv run --no-sync olap prepare tpc_h --scale-factor 10
uv run --no-sync olap prepare parquet_analytics --scale-factor 10
uv run --no-sync olap benchmark duckdb parquet_analytics populate --scale-factor 10
uv run --no-sync olap benchmark duckdb parquet_analytics select --scale-factor 10
```

Substitute `polars` or `clickhouse` for the other registered paths. Run engines
and measured operations serially. The preparation and benchmark commands above
are reproduction instructions, not a record of completed measurements.

The full SF10 input has passed footer, schema, and file-layout checks for all
eight files. Offline contract tests cover source rejection, manifest readiness
and invalidation, DuckDB external views, Polars links and dispatch, ClickHouse
view SQL and read-only mounts, query resolution, Q11 scale substitution, and disk
accounting. They do not establish answers for the complete workload.

```sh
uv run --no-sync ruff format --check olap_benchmarks/suites/parquet_analytics olap_benchmarks/tests/test_parquet_analytics_contracts.py
uv run --no-sync ruff check olap_benchmarks/suites/parquet_analytics olap_benchmarks/tests/test_parquet_analytics_contracts.py
uv run --no-sync pyright olap_benchmarks/suites/parquet_analytics olap_benchmarks/tests/test_parquet_analytics_contracts.py
uv run --no-sync pytest -q olap_benchmarks/tests/test_parquet_analytics_contracts.py
```

The subsequent full-scale campaign completed all 22 queries on DuckDB 587,
Polars 626 and ClickHouse 602, with matching-input peer hashes agreeing.
ClickHouse 602 uses the scoped Q5 written-join-order setting; the original Q5
timeout remains diagnostic. The [follow-up report](../../../docs/campaigns/2026-10-followup.md)
records performance, eligibility and preparation evidence. Peer agreement is
not an independent proof of every answer.
