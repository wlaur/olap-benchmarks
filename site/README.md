# Results viewer

Use Bun for installation and scripts. For the current staged checkout:

```sh
bun install --frozen-lockfile
bun run verify-data
bun run codegen:check
bun run verify
bun run test:e2e --workers=1
```

`results.db` is an ignored local file distributed as `results-{sha256}.db` on the existing GitHub `data` release. `bun run fetch-data` requires the committed manifest’s SHA-256, size and storage contract, fetches only the corresponding hash asset, and verifies it before atomically replacing local `results.db`. Failed downloads preserve the local file, and there is no fallback to the old fixed-name asset. After coordinated publication, fresh checkouts can fetch before verification. `verify-data` requires a declared storage contract and checks the asset's byte size and SHA-256; `codegen:check` checks the schema with the native Node reader. Neither proves that the browser can read the file. `bun run test:reader` performs that check in Chromium using the actual Vite-imported Wasm and worker assets. Install Chromium with `bun x playwright install chromium` if needed.

The current manifest is a **local staging manifest**, pending coordinated publication. Its database was converted from the unchanged selected 2026-10-19 release asset using Python DuckDB 2.0 and `compact_results(db_path=Path("site/public/data/results.db"), storage_version="v1.4.0")`. All other manifest fields, including publication identity, run counts and source/version claims, are preserved. The staged file is 108,015,616 bytes with SHA-256 `d7db73e9a29d01e74c1617e32a5dc744acbaaacf3ff77c083974ed99d4a30e5b`. It intentionally differs from the still-published release asset (115,879,936 bytes, SHA-256 `013b198a4e7fb89a5a5afddde94977460f57b3bfa5adbc03f44e468a69945dc2`). The expected `results-d7db73e9a29d01e74c1617e32a5dc744acbaaacf3ff77c083974ed99d4a30e5b.db` asset has not been uploaded. Do not fetch over this staged file: the expected hash asset is unavailable, so fetching will fail and preserve the local file. The parent’s final corrected campaign publication will replace this staging asset and manifest together, validate and upload the final hash asset, then merge the matching metadata and site code into `main` to trigger the normal Pages workflow. Uploading alone does not dispatch Pages. No release was uploaded by this integration; prior release assets remain untouched.

## DuckDB versions

As verified on 2026-10-21:

- The code generator uses exact `@duckdb/node-api@2.0.0-r.1`, embedding DuckDB **2.0.0**. It opens the published file read-only and closes both connection and instance.
- The browser uses exact `@duckdb/duckdb-wasm@1.33.1-dev57.0`, a **prerelease package**, whose instantiated engine reports **v1.5.4** through both `getVersion()` and `SELECT version()`. MVP and EH assets are bundled locally; DuckDB selects the supported bundle. The package's `1.33.1` number is not an engine version.
- This advances the previous `1.32.0` package's core **v1.4.3**. No published Wasm package embedding final DuckDB 2.0 was available in the upstream evidence. The frontend therefore remains on engine 1.5.4 until an upstream 2.0 Wasm build is published and passes these tests. Do not describe this as Wasm 2.0 or fabricate a package version.

The source evidence is the [published package metadata](https://registry.npmjs.org/@duckdb%2fduckdb-wasm/1.33.1-dev57.0), its [gitHead ef8a4f8912b6e7f62bc0cc490145ebd391b79e1f](https://github.com/duckdb/duckdb-wasm/tree/ef8a4f8912b6e7f62bc0cc490145ebd391b79e1f), and its [core submodule 08e34c447bae34eaee3723cac61f2878b6bdf787](https://github.com/duckdb/duckdb/tree/08e34c447bae34eaee3723cac61f2878b6bdf787). Runtime assertions establish the bundled engine version independently of the package label. The [Node release fetch script](https://github.com/duckdb/duckdb-node-neo/blob/v2.0.0-r.1/bindings/scripts/fetch_libduckdb.py) identifies its native core release.

## Publication format

New publications declare their storage contract in `manifest.json`:

```json
{
  "results_db_format": {
    "storage_compatibility_version": "v1.4.0",
    "minimum_reader_engine_version": "v1.4.0",
    "writer_engine_version": "2.0.0"
  }
}
```

The publisher deliberately compacts with `STORAGE_VERSION 'v1.4.0'` using engine 2.0. Normal benchmark databases and ordinary results compaction retain their native 2.0 format. A 2.0 writer does not imply a browser-readable file: publishing its default native format fails in the current browser engine.

The viewer requires and validates declared versions against the instantiated engine before requesting the database. Missing or malformed declarations and unsupported reader requirements fail explicitly. It never retries with another engine or storage format. The untouched historical 2026-10-19 file was first checked through a separate isolated EH Wasm reader: both version APIs reported v1.5.4, with 244 runs and 127,987 query executions. Its lack of a format declaration is not accepted by the production viewer. The locally staged conversion now supplies the required contract without changing which results were selected.

`test:reader` also creates isolated, tiny fixtures with the pinned native 2.0 reader/writer under ignored `data/duckdb2-upgrade/site-reader-fixtures/`. It verifies that explicit v1.4 storage preserves large IDs, nested JSON, microsecond timestamps, query text, nulls, and failure evidence in the browser, and that native v2.0 storage is rejected even if misdeclared as compatible. Fixture files are removed after the tests; no benchmark workload runs.

Before a dependency upgrade or publication, run `test:reader` alongside asset verification and code generation. Upgrade the exact Wasm pin only after checking both runtime version APIs, the released asset, and 2.0-written fixtures. The Pages workflow installs Chromium and runs `bun run test:reader` before uploading `site/dist`.
