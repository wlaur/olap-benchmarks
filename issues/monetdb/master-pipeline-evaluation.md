# MonetDB master (`pp_hashjoin`) — build, benchmark and findings

**Investigated:** 2026-08-11, re-run 2026-09-22 (see §10)
**Upstream:** `MonetDB` master @ `bbc2d72f02` (2026-08-11 13:39 +0200), re-run at `ee305e491c`
**Baseline:** `Dec2025-SP3` / 11.55.7, ARM64, `macbook-m4-pro`
**Raw data:** `results/master.db` (pipeline off), `results/master-pp.db` (pipeline on),
`results/default.db` (SP3 baseline). All git-ignored.

---

## 1. What landed

Merge `826701de97`, *"The big one: merge pipeline (pp_hashjoin) into default"*, merges a branch
that had been in development since **2021-11-12**: 1,838 commits, 495 files, +48k lines. Authors:
Niels Nes (997), Ying Zhang (434), Joeri van Ruth (242), Sjoerd Mullender (102). Version goes
11.55.7 → **56.0.0**. 2,421 commits separate SP3 from this HEAD.

Three separable pieces.

### 1.1 Morsel-driven pipeline runtime

`monetdb5/mal/mal_pipelines.c`, `monetdb5/modules/mal/pipeline.c`

A guarded block of the MAL program (`Pipelines{mb, stk, start, stop, maxparts, sink}`) is handed
to a persistent worker pool. Each worker runs **the same MAL fragment on a private stack**:

```c
MalStkPtr stk = stack_copy(ma, s->stk, s->start);
p->wid = (int) ATOMIC_INC(&s->workers);
str error = runMALsequence(s->cntxt, s->mb, s->start+1, s->stop, stk, 0, 0);
```

- Morsel size `SLICE_SIZE` = 100,000 rows, handed out by an atomic counter (`pipeline.counter`,
  `pipeline.claim`).
- Workers pinned with `sched_setaffinity`.
- Order-sensitive stages use token passing (`pipeline_pass_token` / `pipeline_get_token`).
- Pipeline breakers are typed sinks (`struct pipeline_io`): `HASH_TABLE`, `SOP`, `TOPN`, `HEAP`,
  `PART`, `MAT`, `COPY`, `COUNTER`, `CONCAT`, `PARQUET`, `MPARQUET`.

This inverts the classic model: mitosis clones the **plan** per data chunk at compile time; this
clones the **execution state** per thread at runtime and lets threads race over the data.

### 1.2 `oahash` — new join hash table

`monetdb5/modules/mal/pp_hash.{c,h}` (~3,500 lines). Open addressing + linear probing, replacing
the bucket-chained `gdk_hash`. `splitmix64` for 64-bit keys; `HT_MIN_SIZE` 8K → `HT_MAX_SIZE` 1G
with rehash; `HT_PRE_CLAIM 256` (claim 256 slots at once to cut contention).

Separately, `faffd946a7` replaced MonetDB's internal atom hashes with **xxhash** — this is why
master hard-requires `libxxhash-dev`.

### 1.3 Sketch-based estimation

`gdk_sketch.c` — HyperLogLog, 6-bit bucket index (64 registers), implementing the `sigma`/`tau`
estimators from Ertl, *New cardinality estimation algorithms for HyperLogLog sketches*
(arXiv:1702.01284) — the **only** citation in the merge. Exposed to SQL as `sys.uniques_guess()`.

Feeds the aggregation strategy choice in `bin_partition_by_slice.c:865`:

```c
if ((BUN)estimate >= 4*GDKL3_size)
    return true;   /* partition by value: private HT per worker, no merge */
return false;      /* two-phase: local pre-aggregation + merge */
```

`GDKL3_size` defaults to 16 MB, tunable via a new `gdk_l3_size` setting.

### 1.4 Also in the merge

- **Native Parquet reader** (`sql/backends/monet5/vaults/parquet/`, ~6,650 lines of hand-written C:
  Thrift metadata parser, Snappy/GZIP/ZSTD/LZ4/LZ4-raw/Brotli, dictionary decoding). Registers as a
  file loader, so `SELECT * FROM '/path/x.parquet'` works, with glob support and `LIMIT` pushdown.
  See §5 — it is not usable yet.
- **`CREATE DISTINCT STRING COLUMN`** — schema-level shared string dictionaries (`sql_ustr`,
  `USTR_DEPENDENCY = 16`) that multiple columns can reference via `USING <dict>`.
- Embedded R and Python removed; `LANGUAGE` keyword removed.
- New `sys.copy_blocksize()` getter/setter, `ncopyintothreads` db property.

### 1.5 The engine is OFF by default

`sql/backends/monet5/rel_physical.c:1685`:

```c
const ATOMIC_BASE_TYPE oahash_enabled = (1U<<19);
if (!SQLrunning || !(ATOMIC_GET(&GDKdebug) & oahash_enabled)
    || gp.complex_modify || gp.cnt[op_except] || gp.cnt[op_inter]) {
    (void)rel_partition(&v, sql, rel);    /* classic mitosis */
} else {
    (void)rel_pipeline(&v, rel, true, 0); /* new engine */
}
```

Bit 19 (524288) is **not** in the documented `GDKdebug` mask list in `gdk/gdk.h` — an undocumented
opt-in. Enable with `mserver5 -d524288`, or at runtime with `SELECT sys.debug(524288)`
(returns the previous value; `sys.debug(0)` disables — note it *sets*, it does not read; use
`sys.debugflags()` to read).

**Critical side effect:** `sql_optimizer.c:139` sets `c->no_mitosis = 1` for all clients whenever
the flag is on. Query shapes the pipeline declines therefore run with *neither* pipeline nor
mitosis parallelism. This is the single biggest source of regressions (§4).

`SQLrunning` is declared in `sql_scenario.h:15` as `// dev. debug var, 2 remove once the code is ~stable`.

---

## 2. Building an ARM64 image

Built from a `git archive` of a local MonetDB checkout rather than a release tarball, so an
arbitrary commit can be built without disturbing the checkout:

```sh
git -C ~/src/MonetDB archive --format=tar --prefix=source/ bbc2d72f02 \
    | gzip -1 > MonetDB-src.tar.gz
docker buildx build --platform linux/arm64 --load --tag monetdb-master:local .
```

The build context is `monetdb-master-build/`, git-excluded because it holds a 97 MB source
tarball and the raw campaign logs.

New requirements vs the SP3 image:

| package | why |
|---|---|
| `libxxhash-dev` / `libxxhash0` | **hard requirement** — cmake fails without it |
| `libsnappy-dev` / `libsnappy1v5` | Parquet codecs |
| `libzstd-dev` / `libzstd1` | Parquet codecs |
| `libbrotli-dev` / `libbrotli1` | Parquet codecs |

New cmake options, all default `ON`: `WITH_SNAPPY`, `WITH_ZSTD`, `WITH_BROTLI`.

Image built and tagged locally as `wlaur/monetdb-container:56.0.0-1`. **It exists on no registry.**

### 2.1 Blocker: fresh databases crash on second boot

Any database created through monetdbd (i.e. the standard container flow) dies on its next start:

```
ERR benchmark[79]: !FATAL: SQLException:sql.grant_func:01007!
                   GRANT: User/role 'public' already has this privilege
```

`sql_update_sep2022` (`sql_upgrades.c:2252`) probes for an EXECUTE privilege on `sys.tracelog`,
finds none, and re-issues a grant that `16_tracelog.sql` already made during bootstrap.
`SQLupgrades` routes the error to `GDKfatal()`. monetdbd retries 5× and gives up.

Standalone `mserver5 --dbpath=...` bootstrap + restart is **not** affected, which is likely why
CI misses it.

**Workaround used here** (`monetdb-master-build/apply-patch.py`): make that one grant tolerant
of `01007`. Touches only startup upgrade code — no planner, executor or ingest code.

No longer needed at current master: six create-and-restart cycles against a build of
`ee305e491c` were clean, with no `FATAL` and no `SIGSEGV` in `merovingian.log`.

⚠️ **Caveat:** the workaround clears the error but the failed statement has already aborted the
upgrade transaction, and `SQLupgrades` continues against it. On roughly 1 in 3 fresh containers the
server then **SIGSEGVs at startup**. All 34 benchmark runs below completed on healthy containers
(verified 0 SIGSEGV in `merovingian.log` each time), but the patched image is not production-stable.
The correct upstream fix is to make the detection query find the existing privilege.

---

## 3. Method

Same suite/scale matrix as the SP3 ARM baseline in `results/default.db`: `kaggle_airbnb`,
`time_series` sf1, `chat_threads` sf1, `tpc_ds` sf1, `rtabench`, `tpc_h` sf10, `clickbench`.

Two passes, each into its own results revision:

| revision | server |
|---|---|
| `master` | `wlaur/monetdb-container:56.0.0-1`, pipeline **off** (stock default) |
| `master-pp` | same image + `MSERVER5_EXTRA_ARGS=-d524288`, pipeline **on** |

Harness changes on branch `monetdb-master-pipeline` (`olap-benchmarks`): `MONETDB_RELEASE` repinned,
plus a `gdk_debug` setting in `dbs/monetdb/settings.py` forwarded via `MSERVER5_EXTRA_ARGS`.
**The release pin must not be merged** — it points at a local-only image. The `gdk_debug` setting is
reusable.

Four unrelated MonetDB development containers holding port 50000 were stopped
for the duration and left stopped, per the owner's instruction.

Query numbers below are `min()` over warm iterations, common queries only.

---

## 4. Results

### 4.1 Warm query totals

| suite | SP3 | master, pipeline **off** | master, pipeline **on** | off vs SP3 | on vs SP3 |
|---|---|---|---|---|---|
| kaggle_airbnb | 145.07 s | 387.95 s | **32.43 s** | 0.37× | **4.47×** |
| clickbench | 109.05 s | 99.15 s | **35.73 s** | 1.10× | **3.05×** |
| chat_threads | 33.60 s | 27.20 s | 39.79 s | 1.24× | 0.84× |
| rtabench | 35.79 s | 12.58 s | 42.47 s | 2.85× | 0.84× |
| time_series | 0.93 s | 0.98 s | 1.07 s | 0.95× | 0.87× |
| tpc_h sf10 | 2.87 s | 4.58 s | 7.37 s | 0.63× | 0.39× |
| tpc_ds sf1 | 2.96 s | 3.46 s | 15.17 s | 0.85× | **0.19×** |

**No single configuration wins.** `rtabench` is the sharpest case: best *and* worst config are both
master (2.85× with the pipeline off, 0.84× with it on).

### 4.2 Per query — violently bimodal

| suite / query | SP3 | pipeline on | |
|---|---|---|---|
| chat_threads `10_threads_with_visualization` | 2629 ms | 113 ms | **23.2× faster** |
| chat_threads `09_keyword_search_global` | 2697 ms | 126 ms | **21.4× faster** |
| kaggle `04_join_three_tables_array_agg` | 71 916 ms | 7347 ms | **9.8× faster** |
| kaggle `05_join_three_tables_row_number` | 69 965 ms | 14 355 ms | **4.9× faster** |
| kaggle `02_join_one_table` | 1157 ms | 3650 ms | 3.2× slower |
| kaggle `03_join_two_tables` | 2027 ms | 7074 ms | 3.5× slower |
| rtabench `0000_terminal_hourly_stats` | 2022 ms | 6216 ms | 3.1× slower |
| rtabench `0001_count_orders_from_terminal` | 1138 ms | 3339 ms | 2.9× slower |
| tpc_h `21_suppliers_waiting` | 171 ms | 3413 ms | **20× slower** |
| tpc_ds `72` | 40 ms | 8462 ms | **211× slower** |
| tpc_ds `98` | 25 ms | **fails** | — |

Note the cluster of mid-size queries landing at almost exactly **0.3×**. That is the `no_mitosis`
fallback (§1.5): queries the pipeline planner declines lose all intra-query parallelism. A flat ~3×
penalty is what that looks like.

### 4.3 Peak server memory (select phase)

| suite | SP3 | off | on |
|---|---|---|---|
| kaggle_airbnb | 33 309 MB | 30 042 MB | **17 363 MB** |
| clickbench | 37 490 MB | 32 840 MB | **42 926 MB** |

The pipeline nearly halves kaggle's footprint but pushes ClickBench to 42.9 GB on a 43.1 GB
machine — its 3.05× speedup comes at essentially the whole box.

### 4.4 ADBC ingest (pipeline off), per table

Splits cleanly by whether the target has a **composite primary key**:

| suite | table | PK | SP3 | master | |
|---|---|---|---|---|---|
| clickbench | hits | none | 308.3 s | 279.4 s | 1.10× |
| rtabench | order_events | none | 131.5 s | 117.9 s | 1.12× |
| chat_threads | chat_message | none | 19.9 s | 15.4 s | 1.29× |
| tpc_h | partsupp | composite | 18.3 s | 15.4 s | 1.19× |
| tpc_h | **lineitem** | `(l_orderkey, l_linenumber)` | 49.1 s | **65.0 s** | **0.76×** |
| rtabench | **order_items** | `(order_id, product_id)` | 14.1 s | **20.0 s** | **0.70×** |

The two large composite-PK tables regressed — the constrained-append path, consistent with the
RTABench PK carve-out issue tracked separately in the driver's review notes.

TPC-H's *overall* populate still improved (157.0 → 136.3 s) only because the **restart** phase went
65.8 → 34.7 s (1.89×, a WAL/checkpoint win) — masking a 32% ingest regression. Do not quote whole-
populate numbers as ingest numbers.

Likely cause of the non-PK improvements: `915292041d`, "synchronize on claim but also on first
append… reduces the use of BATupdates".

---

## 5. Parquet reader — not usable

Answering the original question (*is the Parquet path better than the tuned ADBC ingest?*): **no,
and not close.** It cannot correctly read either benchmark dataset.

Requires `--loadmodule=parquet` at **bootstrap** (the `sys.parquet_*` SQL functions come from
`76_parquet.sql`, installed only on the first boot; `MDB_DB_PROPERTIES` is applied too late).
`SELECT * FROM 'file.parquet'` works with just the module loaded, since `fl_register()` happens in
the MAL prelude.

| test | result |
|---|---|
| `count(*)` on `hits.parquet` (metadata only) | ✅ 99,997,497 in 1.29 s |
| Full ingest of `hits` (100M rows, 14.8 GB) | 💥 **SIGSEGV** ~100 s in |
| `SELECT ... LIMIT 1` on `lineitem.parquet` | 🔒 **deadlock** — 7 min, 3% CPU, never returned |
| `INSERT ... SELECT *` into real ClickBench schema | ❌ `types bigint(63,0) and timestamp(7,0) are not equal for column 'EventTime'` |

Reader-inferred schema for `hits.parquet`:

```
WatchID     bigint     ok
EventTime   bigint     WRONG - should be timestamp
EventDate   smallint   WRONG - should be date (int32 epoch-days into 16 bits)
e1          varchar    WRONG - column 14, real name discarded
```

Causes, all in source:

- **`pqc_reader.c:1406`** — dictionary decode handles precision ∈ {8,16,32,64,96,128} only.
  Anything else hits `else { printf("later %d\n", ...); }`, **fills no output, and returns `nrows`
  as if it succeeded**. TPC-H `DECIMAL(15,2)` → precision 15 → server logs `later 15`, pipeline
  waits forever on a source that never produces.
- **`parquet.c:451`** — `if (i == 14) name = ma_strdup(sql->sa, "e1");` unconditionally. Confirmed
  empirically. Positional `INSERT ... SELECT *` is unaffected; anything by name breaks.
- TIMESTAMP / DATE logical types not mapped to their SQL counterparts.
- `parquet.c:~410` — the "is this tabular / has repetition" validation is dead code behind `if (0)`.

Also note it is **server-side only**: the file must be on the server's filesystem. Even once
correct, it is not a drop-in replacement for a client-streaming ingest path.

Other WIP markers: `printf("#using large fallback\n")` and `printf("#est == 0, %d\n")` in
`rel_pphash.c:32,40`; `printf("# todo needs check\n")` etc. in `rel_physical.c:899,1069,1080`.

---

## 6. New defects found (filable)

Rough severity order. Each has since been written up as an individual report alongside the
driver, which carries the authoritative status: several were re-verified against master
`ee305e491c` on 2026-09-22, and two are now fixed.

1. **Fresh databases created via monetdbd crash on second boot** (§2.1). Deterministic. Blocks all
   container use of master.
2. **Parquet full ingest SIGSEGVs** — ~100 s into ClickBench `hits` (100M rows).
3. **Parquet deadlocks on `DECIMAL` precision ∉ {8,16,32,64,96,128}** — `pqc_reader.c:1406` reports
   success while producing nothing.
4. **TPC-DS Q98 fails with the pipeline enabled.** Client-visible error:
   `ArrowInvalid: External error: column has 10064 bytes; expected 5032` — exactly 2×. The server's
   binary result buffer disagrees with its own declared schema. Q98 projects decimal arithmetic that
   can promote to 128-bit, so most likely a column declared 8-byte filled with 16-byte values.
   Passes on SP3 and on master with the pipeline off. Any binary-protocol client would hit this.
5. **Parquet maps TIMESTAMP → `bigint`, DATE → `smallint`.**
6. **Parquet renames column 14 to `e1`** unconditionally.
7. **`no_mitosis` fallback costs ~3×** on every query the pipeline planner declines (§4.2). Design
   issue rather than a bug, but it is what makes the flag unusable as a global switch.

### 6.1 Regression check against the issue reports

The reports themselves are tracked alongside the driver, which also carries the script that
re-runs every check and the authoritative status table. The snapshot below was re-verified
2026-09-22 against `master` @ `ee305e491c`.

| issue | SP3 | master 56.0.0 |
|---|---|---|
| `sys.sessions` segfaults for non-admin users | crash | ❌ **still crashes** — see below |
| wrong `avg()` over `ROWS` frame wider than 16 | `12.75` at i=17 | ❌ identical (expected `9.0`) |
| empty result, `ORDER BY` + `CASE` over `grouping()` | silent empty | ❌ identical |
| narrowing `VARCHAR` leaves over-long data | 900 chars in `VARCHAR(255)` | ❌ identical |

**Correction (2026-09-22).** This table originally recorded the `sys.sessions` crash as fixed
on master. It is not. The check behind that verdict created a non-admin user and read
`sys.sessions`, but never created a `RUNCLIENT` whose `sqlcontext` is still `NULL`, so the
dereference could not fire. Holding an `mclient -l mal` session open makes it segfault
deterministically at `ee305e491c`, and the unguarded deref is still at `sql.c:3756`.

Watch out when re-testing the window issue: a `WHERE i BETWEEN ...` filters rows *before* the
window function runs, so the frame never widens and the bug does not appear. Compute the
window in a subquery and filter outside it.

---

## 7. Client / driver impact

The pipeline work itself is server-internal. But a **separate change in the same merge window
breaks byte-exact `&4` parsers**.

`3c0c525b6f` (2026-08-06, upstream #7988) in `sql_scenario.c`:

```c
-   mnstr_printf(be->out, "&4 %s\n", post_autocommit ? "t" : "f");
+   mnstr_printf(be->out, "&4 %c %c\n", autocommit_flag, error_message_follows_flag);
```

`&4 t` → `&4 t t`. Companion commit `dc76e50adb` also makes the notification fire in more
situations, including after a failed transaction. Upstream's own test had to be version-gated on
pymonetdb ≥ 1.9.1 (`b301024a95`), so the reference client needed changes too.

`monetdb-rust` matched exactly in three places and broke: hard `InvalidHeader` from the general
reply-parser dispatch, hard `UnexpectedHeader` from `set_autocommit()`, and silent stale state in
`response_autocommit()`.

**Fixed** in `monetdb-rust` `400e28b` (PR #27, merged): one `parse_autocommit_line()` helper anchored
on the `&4` header at line start, reading only field 1 and ignoring trailing fields. Verified with
the full CI integration suite against both server builds — 58/58 on SP3, 58/58 on master.
The ADBC driver picks it up via a submodule bump.

Why the benchmark never hit it: `&4` is only emitted when autocommit *changes* during a statement.
The harness holds one mode per connection. Normal ADBC/SQLAlchemy usage that toggles autocommit or
mixes explicit transactions triggers it immediately.

Beyond `&4`, the SP3→HEAD client-facing diff is small and internal (`mapi.c` +104, `sql_result.c`
+79, stream refactors).

---

## 8. Release timing (inference, not announced)

Tag cadence: Mar2025 → Dec2025 was 8.5 months; SP1/SP2/SP3 came 10/7/10 weeks apart, SP3 on
2026-06-15.

- **Dec2025-SP4: likely late Aug – Sept 2026**, 11.55.x bug fixes, none of this in it.
- **The pipeline as a stable default: not in Aug/Sept.** Dec 2026 at the earliest, more realistically
  Mar 2027, and probably still opt-in.

Reasoning: the merge landed 2026-08-11 and catalog upgrade code was still being written that day
(`3ad2bbdcf9`, `6a2f5506ab`, `bbc2d72f02`); fresh databases do not start at all; debug `printf`s
remain in planner paths; and the gating work has not begun — the
[2024 roadmap](https://www.monetdb.org/about-us/roadmap-2024/) promises *"new optimisers to
determine the best execution strategy for a given query"*, and §4 shows why that is mandatory rather
than nice-to-have. Today that chooser is one global boolean that also disables mitosis.

The same cycle removes embedded R/Python and the `LANGUAGE` keyword — user-visible removals needing
a major release with migration notes.

---

## 9. Bottom line

The engine does what it claims on the workloads it targets: **3–4.5× on ClickBench and
kaggle_airbnb**, up to **23× on individual string-heavy join/window queries** — precisely where
MonetDB was ~30× behind DuckDB. On kaggle it closes an 18× gap to ClickHouse down to 4×, and on the
heaviest query (`04`) from 16× behind to 1.7×.

It also makes five of seven suites slower, breaks one TPC-DS query, and takes ClickBench to the
memory ceiling. The remaining kaggle gap is mostly the `no_mitosis` fallback, not engine quality —
fix that and kaggle lands around 12–13 s (~1.5× behind ClickHouse rather than 4×).

Per-query-shape engine selection is the blocking work before a feature release. It is not polish.

### Reference

- Leis, Boncz, Kemper, Neumann, *Morsel-Driven Parallelism*, SIGMOD 2014 — https://db.in.tum.de/~leis/papers/morsels.pdf
- Boncz, Zukowski, Nes, *MonetDB/X100: Hyper-Pipelining Query Execution*, CIDR 2005 — https://www.cidrdb.org/cidr2005/papers/P19.pdf
- Manegold, Boncz, Kersten, *Optimizing Main-Memory Join on Modern Hardware*, IEEE TKDE 14(4) 2002 — https://ir.cwi.nl/pub/11143/11143B.pdf
- Balkesen, Teubner, Alonso, Özsu, *Main-Memory Hash Joins on Multi-Core CPUs*, ICDE 2013 — https://dblp.uni-trier.de/rec/conf/icde/BalkesenTAO13.html
- Birler, *Simple, Efficient, and Robust Hash Tables for Join Processing* — https://db.in.tum.de/~birler/papers/hashtable.pdf
- *Global Hash Tables Strike Back! An Analysis of Parallel GROUP BY Aggregation*, PVLDB — https://dl.acm.org/doi/10.14778/3778092.3778110
- Ertl, *New cardinality estimation algorithms for HyperLogLog sketches*, arXiv:1702.01284 — https://arxiv.org/abs/1702.01284
- MonetDB roadmap 2024 — https://www.monetdb.org/about-us/roadmap-2024/

No publication describes this engine itself; it appears to be engineering rather than a paper artifact.

---

## 10. Re-run against `ee305e491c` (2026-09-22)

Same machine, same suite/scale matrix, same Dec2025-SP3 baseline. Revisions `tip` (pipeline
off) and `tip-pp` (pipeline on). 2,400+ commits of engine work separate this from SP3 and
roughly 400 from the August build.

### 10.1 Pipeline off — MonetDB's shipped default

Warm totals, common queries, versus SP3:

| suite | SP3 | `tip` off | vs SP3 | `76da78f` off, for reference |
|---|---:|---:|---:|---:|
| rtabench | 35.79 s | 12.88 s | **2.78×** | 2.80× |
| chat_threads | 33.60 s | 28.35 s | 1.19× | 1.20× |
| clickbench | 109.05 s | 95.21 s | 1.15× | 1.12× |
| time_series | 0.93 s | 1.01 s | 0.93× | 0.94× |
| tpc_ds | 2.96 s | 3.61 s | 0.82× | 0.80× |
| tpc_h sf10 | 2.87 s | 4.51 s | 0.64× | 0.63× |
| kaggle_airbnb | 145.07 s | 363.12 s | **0.40×** | 0.49× |

Essentially unchanged since August. In the configuration MonetDB actually ships, this is still
a net regression against SP3 on four of seven suites, and nothing in the intervening commits
has moved it. kaggle drifted slightly worse. The one real win, rtabench's 2.78×, coexists with
19 of its 41 queries being more than 20% *slower* — one or two large queries carry the total.

### 10.2 Pipeline on, with ClickHouse and DuckDB

Only four suites have a comparable query set, because the other three hit corrupt results with
the engine enabled (§10.3):

| suite | n | SP3 | off | on | ClickHouse | DuckDB | on vs SP3 | best vs DuckDB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| kaggle_airbnb | 5 | 145.07 s | 363.12 s | 39.95 s | 8.08 s | **4.27 s** | 3.63× | 0.11 |
| clickbench | 43 | 109.05 s | 95.21 s | 69.47 s | 13.87 s | **9.83 s** | 1.57× | 0.14 |
| chat_threads | 23 | 33.60 s | 28.35 s | 41.04 s | 9.55 s | **1.99 s** | 0.82× | 0.07 |
| time_series | 59 | 0.93 s | 1.00 s | 1.07 s | 1.59 s | **0.88 s** | 0.86× | 0.88 |

ClickHouse 26.7.1.1315, DuckDB 1.5.5, same machine and scale factors, identical query sets.

Both headline wins shrank against the August build: ClickBench 3.05× → 1.57×, kaggle
4.47× → 3.63×. The gap the engine exists to close is still wide — MonetDB's *best* configuration
is 7× to 14× slower than DuckDB on three of these four suites.

### 10.3 The engine returns corrupt results

Three queries, three suites, all with `-d524288`, all passing on the same build without it:

| suite / query | error |
|---|---|
| tpc_ds `98` | `ArrowInvalid: column has 10064 bytes; expected 5032` |
| tpc_h `07_volume_shipping` | `DataError: INVALID_DATA: invalid utf-8 encoding in result set` |
| rtabench `0017_top_selling_month_product` | `OperationalError: IO: unexpected end of file` |

The server does not crash: no `SIGSEGV` and no `SIGABRT` anywhere in `merovingian.log`, and
queries continue normally after a reconnect. The third error is downstream of a malformed
result rather than a separate fault.

Q98 is deterministic and reproduces on an idle machine in under a minute, byte-identical to the
August build. The other two appeared only after about an hour of sustained load and did not
reproduce in isolation.

Every one was caught only because the corruption was *structurally* invalid. Corruption that
still parses would have been recorded here as a passing query with wrong numbers. Value-level
validation of pipeline-on results against pipeline-off results is the check this campaign could
not make and the next one should.

### 10.4 Bottom line

The August conclusion stands, with the ceiling lower than it looked. Per-query-shape engine
selection is still the blocking work, and correctness now ranks ahead of it: an engine that
silently corrupts results is not one a `GDKdebug` bit should be able to turn on.
