# MAX of an empty designated timestamp returns no row

Observed in the saved r5 fixture on 2026-10-22 (Europe/Helsinki), using QuestDB
10.0.1, Linux aarch64, JDK 25.0.2. Both servers' `build()` readbacks identify
commit `7a391566ac827e0d8c269b91f2415c16b0a32a84`. This report reviews retained
evidence and source only; no new engine statements were executed.

## Observed versus expected

The exact executed query on both baseline and candidate was:

```sql
select max(time) as latest from proof_wide_empty
```

Both tables were empty. Both answers have one column, `latest`, with Arrow
schema `latest: timestamp[us]` and Polars type
`Datetime(time_unit='us', time_zone=None)`. The baseline returns `[[null]]`
(one row, one NULL); the candidate returns `[]` (zero rows). Expected: one
NULL row for this aggregate without grouping, as the baseline and independent
fixture expectation agree. Reopening the saved Arrow files confirms both
cardinalities. The recorded answers came through ConnectorX; a second query
transport was not used to independently reproduce the mismatch.

The empty table's full result has the same schema on both sides:
`time: timestamp[us], binary_1: bool, ratio_1: float, deviation_1: float,
process_4: float`. The separate ordinary aggregate over `process_4` returns
`[[0, 0, null, null, null, null]]` on both sides for
`count(*), count(process_4), sum(process_4), avg(process_4), min(process_4),
max(process_4)`.

Captured baseline plan:

```text
Async Group By workers: 10
  vectorized: true
  values: [max(time)]
  filter: null
    PageFrame
        Row forward scan
        Frame forward scan on: proof_wide_empty
```

Captured candidate plan:

```text
Limit value: 1 skip-rows: 0 take-rows: 0
    SelectedRecord
        PageFrame
            Row backward scan
            Frame backward scan on: proof_wide_empty
```

## Executed fixture and derived minimal reproduction

The executed fixture used a generated, typed, zero-row Parquet file. These are
the saved creation statements, with their actual filenames:

```sql
create table proof_wide_empty as (
    select * from read_parquet('proof_wide_empty_60a90e9ccb4f4fd5aed528d58531fbe6.parquet')
)
```

```sql
create table proof_wide_empty as (
    select * from read_parquet('proof_wide_empty_7580203dd93b4875a246ce506fec34b5.parquet') limit 0
) timestamp(time) partition by DAY WAL;
insert into proof_wide_empty
select * from read_parquet('proof_wide_empty_7580203dd93b4875a246ce506fec34b5.parquet');
```

The first block ran on the baseline server and the second on the candidate.
The temporary Parquet files were removed by the loader; the statements and
answers remain saved. The runner linked below records their construction.

The following self-contained reduction is **derived and unexecuted**. It
removes the Parquet import and unrelated columns; its results are predictions,
not additional observations. Submit each statement separately to a fresh
QuestDB 10.0.1 instance, for example through the SQL console:

```sql
CREATE TABLE empty_plain (time TIMESTAMP);
CREATE TABLE empty_native (time TIMESTAMP)
    TIMESTAMP(time) PARTITION BY DAY WAL;

SELECT max(time) AS latest FROM empty_plain;
SELECT max(time) AS latest FROM empty_native;
EXPLAIN SELECT max(time) AS latest FROM empty_native;

DROP TABLE empty_native;
DROP TABLE empty_plain;
```

Expected: both SELECTs return one TIMESTAMP NULL row. The saved fixture and
source suggest the second SELECT instead returns zero rows with the backward
scan plan, but this reduced DDL has not been verified.

## Source inference and causal limits

At the tested commit,
[`SqlOptimiser.rewriteSingleFirstLastGroupBy`](https://github.com/questdb/questdb/blob/7a391566ac827e0d8c269b91f2415c16b0a32a84/core/src/main/java/io/questdb/griffin/SqlOptimiser.java#L11485-L11559)
recognizes a single aggregate of the designated timestamp on a direct table,
without grouping keys or SAMPLE BY. For `max`, it replaces the aggregate
expression with the timestamp column, changes the model to
`SELECT_MODEL_CHOOSE`, adds descending timestamp order, and sets a limit of
one. This method contains no empty-input row synthesis. A projection limited
to one existing row cannot supply the aggregate's NULL row when none exists.
This is a concrete source path consistent with the captured plan, not an
instrumented confirmation of the runtime cause or a tested fix.

QuestDB's [designated timestamp documentation](https://questdb.com/docs/concepts/designated-timestamp/)
describes timestamp ordering and interval scans. In r5, the eight completed
candidate nonempty wide states proved designated `time`, DAY partitioning,
healthy caught-up WAL, unchanged native `TIMESTAMP, BOOLEAN, FLOAT, FLOAT,
FLOAT` types, and interval range plans. The candidate changed designation,
partitioning, WAL, and creation procedure together. Their individual necessity
was not isolated. The empty candidate's catalog was not collected because
answer validation stopped first; its DDL, full schema, answers and plans were
saved. No candidate EAV, boundary-state, or NULL-time rejection proof completed.

## Evidence and disposition

Evidence root:
[`quest-native-fixture-resume-20261022-r5/`](../../data/fairness-audit/native-polars-proposals/questdb-serial-proof/runs/quest-native-fixture-resume-20261022-r5/).
Under that root, both `original/` and `candidate/` contain
`proof_wide_empty-initial-{latest,explain-latest,all,aggregate}.{json,arrow}`,
`proof_wide_empty-insert.json`, `adapter-sql.jsonl`, and `runtime-build.json`.
`failure-comparison.json` records the exact mismatch;
`manual-review.json`, `evidence-index.json`, and `final-ledger.json` record
scope, evidence hashes and stopping conditions. The executed helper is
[`runner_live_r5.py`](../../data/fairness-audit/native-polars-proposals/questdb-serial-proof/runner_live_r5.py).

The earlier r4 stop was a helper FLOAT NULL sorting-oracle defect, corrected
in r5. This r5 failure is a separate saved answer-cardinality mismatch after
22 baseline states and eight candidate nonempty wide states passed. One layout
pattern consumed 423.288579 seconds of bounded diagnostics across r4/r5;
776.711421 seconds remained unused. There was no retry after this failure.

The candidate was not adopted because the correctness gate failed. Canonical
SQL remains unchanged; no speculative query or schema workaround was added.
No speedup or full-scale resource conclusion follows from this fixture.
The next valid gate requires a fix or version change, then the complete
fixture matrix, followed by full-scale correctness and resource/performance
proof before adoption. No upstream confirmation, filed upstream issue, fixed
version, or tested fix is established here.
