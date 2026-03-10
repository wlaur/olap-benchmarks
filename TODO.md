## Insert Redesign Plan

### Goal

Redesign insert handling so benchmark population does not require fully materializing input data in Python memory.

Primary target:

- Support `pl.LazyFrame` inputs, especially `pl.scan_parquet(...)`
- Preserve source row order from the Parquet file
- Do not introduce a pre-insert sort step
- Start with MonetDB, then extend the pattern to the other databases

### Current bottleneck

The shared database API currently requires `pl.DataFrame` for `insert()`, and suite population paths eagerly materialize Parquet inputs before inserting. That forces large datasets to be decompressed into Python memory.

Examples:

- `time_series` uses `pl.read_parquet(...)`
- `rtabench` uses `pl.read_parquet(...)`
- `kaggle_airbnb` uses `pl.read_parquet(...)`
- `clickbench` starts lazy, but calls `.collect()` before insert

### Proposed public API

Introduce a shared insert input type:

```python
type InsertData = pl.DataFrame | pl.LazyFrame
```

Update database insert signatures to accept `InsertData` instead of only `pl.DataFrame`.

Notes:

- Keep `pl.DataFrame` support for small or already-materialized inputs
- Use `pl.LazyFrame` for benchmark population from Parquet
- `upsert()` can remain `DataFrame`-only for now unless there is a clear need to redesign it too

### Shared design principles

1. Schema should come from the input without collecting the full dataset.
   - `DataFrame`: `df.schema`
   - `LazyFrame`: `lf.collect_schema()`

2. Insert code should choose a backend-specific execution strategy.
   - batch iteration with `collect_batches(...)`
   - staged file via `sink_parquet(...)`
   - staged file via `sink_csv(...)`

3. Preserve source row order.
   - Do not add a sort before insert
   - If a backend later needs a sorted physical layout for performance, that should be a separate, explicit design decision

4. Logging should stop assuming full-frame shape is always known up front.
   - For lazy paths, count rows during batching or compute row count lazily only when needed

### MonetDB first

#### Why MonetDB needs a custom lazy path

MonetDB currently inserts by writing one binary file per column and then running `COPY LITTLE ENDIAN BINARY INTO ...`.

That path is good and should be preserved, but the current implementation requires a full `DataFrame` because it serializes each entire column at once.

#### Target MonetDB design

1. Accept `pl.LazyFrame` in `MonetDB.insert(...)`
2. Resolve schema from `collect_schema()`
3. Create the target table from schema before loading when needed
4. For lazy input:
   - iterate using `collect_batches(chunk_size=...)`
   - keep column order fixed from schema
   - append each batch's bytes to the same per-column temporary binary files
5. After all batches are written:
   - run the existing MonetDB binary `COPY` statement
   - commit
   - clean up temporary files

#### Required refactor for MonetDB binary writers

Current binary helpers write a full file from a `pl.Series`.

Refactor toward:

- `serialize_binary_column_data(series: pl.Series) -> bytes`
- `write_binary_column_data(series: pl.Series, path: Path) -> None`

Then:

- existing eager path can keep using `write_binary_column_data(...)`
- lazy path can append `serialize_binary_column_data(...)` output to the existing file handle

This should work because the current MonetDB binary column encodings are row-concatenated and do not appear to require a file footer.

#### MonetDB implementation checklist

- Add shared insert input type alias
- Update abstract `Database.insert(...)` signature
- Update `MonetDB.insert(...)` signature
- Add a helper to get schema from `DataFrame | LazyFrame`
- Add a helper to iterate batches from `DataFrame | LazyFrame`
- Refactor MonetDB binary writers to support chunk serialization
- Implement lazy MonetDB insert path with batch appends
- Keep existing eager MonetDB insert path for already-materialized inputs
- Add row-count accumulation during batching for logging
- Make batch size configurable

#### MonetDB testing

- Small mixed-type dataset inserts correctly from `DataFrame`
- Same dataset inserts correctly from `LazyFrame`
- Fetch results are identical between eager and lazy insert
- Null handling is preserved for all supported MonetDB types
- Column order is preserved exactly
- Existing table insert still requires exact schema/order match

### Suite changes after MonetDB support lands

#### `time_series`

- Replace `pl.read_parquet(...)` with `pl.scan_parquet(...)`
- Pass lazy input directly to `db.insert(...)`
- Preserve Parquet row order

#### `rtabench`

- Replace `pl.read_parquet(...)` with `pl.scan_parquet(...)`
- Pass lazy input directly to `db.insert(...)`

#### `kaggle_airbnb`

- Replace `pl.read_parquet(...)` with `pl.scan_parquet(...)`
- Pass lazy input directly to `db.insert(...)`

#### `clickbench`

- Change `load_dataset()` to stay lazy all the way through insert
- Keep timestamp/date conversion in the lazy plan
- Avoid `.collect()` before insert

### Backend rollout after MonetDB

#### DuckDB

Current code already has a file-backed insert mode.

Plan:

- accept `LazyFrame`
- stage to Parquet with `sink_parquet(...)`
- insert from the staged Parquet file

#### ClickHouse

Current code already writes temporary Parquet and inserts from `file(..., Parquet)`.

Plan:

- accept `LazyFrame`
- use `sink_parquet(...)` for the non-partitioned case
- for partitioned insert mode, add lazy staging that writes multiple Parquet files without collecting the whole dataset

#### QuestDB

Current code already has a Parquet-based insert path and a clickbench path that stays lazy until `sink_parquet(...)`.

Plan:

- accept `LazyFrame`
- stage transformed lazy input to Parquet
- insert via `read_parquet(...)`

#### Postgres

Current code writes CSV then uses `timescaledb-parallel-copy`.

Plan:

- accept `LazyFrame`
- write staged CSV with `sink_csv(...)`
- run `timescaledb-parallel-copy` against that CSV

#### TimescaleDB

Same as Postgres.

Plan:

- accept `LazyFrame`
- write staged CSV with `sink_csv(...)`
- run `timescaledb-parallel-copy`
- do not sort before insert

### Open questions

1. What should the default lazy batch size be for MonetDB?
   - Wide tables need much smaller row batches than narrow tables

2. Should we expose a backend-agnostic staging helper?
   - Maybe later
   - Not needed for the first MonetDB implementation

3. Should `upsert()` also accept `LazyFrame`?
   - Probably not in the first pass

4. Should row count always be computed for lazy inputs?
   - Prefer counting during batch iteration when possible

### Suggested implementation order

1. Introduce `InsertData = pl.DataFrame | pl.LazyFrame`
2. Update abstract and concrete `insert()` signatures
3. Implement lazy MonetDB insert with batch-based binary file generation
4. Add MonetDB tests for eager vs lazy equivalence
5. Switch `time_series` to `scan_parquet(...)` for MonetDB
6. Extend the same insert contract to the other suites
7. Roll out backend-specific lazy implementations for DuckDB, ClickHouse, QuestDB, Postgres, and TimescaleDB
