# Database issues

Bug reports and investigations for the database engines this suite benchmarks, one
subdirectory per engine. These are notes about *other people's software*, written so they can
be filed upstream — not a backlog for this repository. Work items for the benchmark suite
itself belong in [`../REVIEW.md`](../REVIEW.md).

| Engine | Contents |
| --- | --- |
| [`clickhouse/`](clickhouse/) | [`FixedString` deserialization failure under `parallel_hash`](clickhouse/fixedstring-too-large-under-parallel-hash.md) |
| [`monetdb/`](monetdb/) | [Pipeline-engine evaluation](monetdb/master-pipeline-evaluation.md) — build, benchmark and findings for the `pp_hashjoin` engine |

## Conventions

- One file per issue, named for the defect rather than the engine: the directory already says
  which engine it is. `fixedstring-too-large-under-parallel-hash.md`, not `CH_ISSUE.md`.
- Each file states the versions tested, a self-contained reproduction, and what was observed
  versus expected. A reader who has never seen this repository should be able to run it.
- Reproductions use disposable containers and public datasets. A reproduction that needs a
  populated benchmark database says so and explains how to get one.
- Keep a file after the defect is fixed, and record which version fixed it. Knowing when
  something was fixed is what version pins get written against.
- Nothing engine-specific goes at the repository root.
- Defects in a database *driver* are tracked with that driver. This directory is for the
  engines themselves, and for investigations that only mean something next to this harness.
