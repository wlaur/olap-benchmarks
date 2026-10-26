# Deferred timeout recovery, 26 October 2026

Prepared for the user's explicit instruction to run the deferred work. Revision
`2026-10-timeout-recovery-deferred-20261026` is a fresh campaign, with a new
journal and database. The runner is unchanged. Its clean documentation commit
is the measurement HEAD; no commits or branch/tree moves occur after START.

## Scope and order

1. PostgreSQL TPC-DS SF1: fresh complete population and all 99 queries.
2. TimescaleDB TPC-DS SF10: fresh complete population and all 99 queries.
3. PostgreSQL TPC-DS SF10: fresh complete population and all 99 queries.
4. MonetDB ClickBench SF1: fresh complete population and all 43 queries.
5. PostgreSQL TPC-H SF10: fresh complete population and all 22 queries, then
   the separately requested analytical 16-client retry only.

The first four Cell objects equal the previous final plan's cells[1:]. The last
Cell equals the original plan's PostgreSQL TPC-H SF10 Cell. There are 362 SELECT
slots and one analytical run. Original settings, pins, all nine input manifests,
SQL, schemas, exclusions and input directory remain frozen. All analytical
fields equal the original contract except clients=[16]. This is one explicitly
authorized retry after fresh prerequisites. Prior successful clients1/4 and
failed client16 remain separate historical attempts. No automatic retry follows
another failure.

The prior TimescaleDB TPC-DS SF1 full cohort final/run686 is retained as a whole:
Q11 failed but its other 98 queries and validation passed. The interrupted
PostgreSQL final/run688 remains failed and validation-incomplete. It is not
resumed, acknowledged or reinterpreted. Its new replacement begins from a new
full population. Qualified revision/run IDs are mandatory because the original
seed starts this revision at run685 again.

## Frozen measurement contract

Use 10 CPUs, 24 GiB engine memory, 28 GiB sampled client memory, a 300-second
query budget, unchanged 30-second SELECT cancellation grace, 14,400-second
operation budget and 40 GiB runtime disk reserve. Analytical watchdogs retain
their original behavior and have no SELECT grace. Require at least 155 GiB free
at START, target 158 GiB, and a quiet host. Do not run tests, installs, competing
engines or large unrelated I/O during measurement.

TPC-DS uses the adopted Q31/Q65/Q85 outer byte-order collation and Q49 binary64
ratio contract. The historical exact-decimal result is not declared wrong.
MonetDB's actual SELECT session must record querytimeout=300 seconds. The full
43-query cohort, Q32 timeout recovery and zero post-run session baseline remain
live checks; the old 2-second synthetic proof does not establish these. If Q32
fails cleanly, Q33-Q42 must execute in the same attempt. An outer 330-second
watchdog or cascading connection errors stop for review. ClickBench hits must
contain exactly 99,997,497 rows; Q28 runs and is timed but retains its existing
cross-engine answer-comparison exclusion.

The analytical retry preserves all 22 TPC-H queries, seed0, requested60s,
client16, warmup and answer rules. Population and SELECT must complete and
validate first. Analytical warmup failure remains a hard prerequisite failure;
no success, QPS or percentile claim is made without measurement evidence.

## Evidence and gates

The seed is an independent mode0444 regular file, 143,929,344 bytes, SHA256
832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f.
It is restored from the checksum-addressed original evidence archive, never
from the new published dataset or a recovery database. Rehash the archive's
827 objects and use its manifest to preserve original namespaces. Reuse the
102-artifact read-only validator baseline only after rehashing every entry and
confirming the required DS1/DS10/CB1/TH10 findings arrays are empty. Preserve
all raw historical statuses and source heads through the published archives.

Before START verify source/SQL/schema/contract equality, Python3.14.3 and the
129-package frozen lock, all three pinned images by ID and digest without
pulling, owned seed identity, input names/sizes and full hashes, absent owned
outputs and containers/processes, and the 155 GiB floor. Record the actual HEAD,
plan hash, archive restoration, environment and images in the ignored handoff.
The runner repeats full input equality before each population.

At any terminal failure, the runner stops and performs owned cleanup. Review
raw run status, full saved validation, provenance, coverage/unstarted markers,
resource failures, and current writer/container absence before another action.
A clean validated failed SELECT can advance only through the existing explicit
--resume --accept-failed-select flag after a concrete recorded parent review and
an independent sealed checkpoint. No raw status or successful-query splicing is
allowed. Validation failure, failed analytical prerequisite, outer watchdog,
connection-recovery failure or unexplained integrity failure stops for review;
none qualifies for an automatic acknowledgment. Failed population needs the
original explicit reasoned review rule. Plain --resume is not a retry permission.
The user authorized the whole deferred task, so routine supported dispositions
still require concrete evidence review but not another permission question.

Owned outputs are results/2026-10-timeout-recovery-deferred-20261026.db,
data/temp/timeout-recovery/, and this worktree's engine stores/scratch. Shared
inputs, unrelated containers, user caffeinate and other worktrees are untouched.
Each terminal prefix is sealed before any supported continuation. Archive exact
measurement HEADs and raw evidence before final cleanup. Publication continues
to use whole attempts, explicit source generations and existing v2 certification;
this campaign never changes the scoring formula or certifies itself implicitly.

## Exact commands

```sh
uv run --no-sync python scripts/timeout_recovery.py check --plan docs/campaigns/2026-10-timeout-recovery-deferred-plan.json
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run --plan docs/campaigns/2026-10-timeout-recovery-deferred-plan.json
```

START is authorized by the user's latest instruction after fresh gates pass.
No old START/RELEASE flags or journals are reused. The current published asset
remains unchanged until terminal whole-attempt review and an atomic publication.
