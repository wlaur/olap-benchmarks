# Frontend Architecture

Static web app for exploring OLAP benchmark results. Hosted on GitHub Pages, queries data client-side using DuckDB WASM.

## Tech Stack

| Layer              | Choice                    | Notes                                              |
|--------------------|---------------------------|----------------------------------------------------|
| Runtime / pkg mgr  | Bun                       | Fast installs, used as script runner               |
| Bundler / dev      | Vite                      | Mature React HMR, first-party Tailwind v4 plugin   |
| UI framework       | React + TypeScript        |                                                    |
| Styling            | Tailwind CSS v4           | `@tailwindcss/vite` plugin, no config file needed  |
| Data layer         | @duckdb/duckdb-wasm       | `ATTACH` the .duckdb file over HTTP                |
| Charts             | Recharts                  | D3-based, native React components, ~180KB          |
| Tables             | TanStack Table v8         | Headless, styled with Tailwind, ~30KB              |

## Data Flow

1. `olap publish` copies the results DuckDB file to `site/public/data/results.duckdb`
2. Vite copies `public/` as-is to `dist/` on build
3. Frontend loads DuckDB WASM, runs `ATTACH '<base>/data/results.duckdb'`
4. All queries run client-side in the browser via SQL

GitHub Pages serves CORS headers by default, so `ATTACH` over HTTP works without extra config.

## Data Model

Source schema: `olap_benchmarks/results_models.py`

Key tables:
- `run` — one row per benchmark execution (suite, db, db_version, operation, system, status, timing)
- `run_step` — individual steps within a run (phases and queries with timing, row counts)
- `run_metric` — time-series resource metrics per run (cpu, memory, disk)

### Top-Level Dimension: `system`

The `system` column on `run` identifies the machine that produced the results (e.g. `macbook-pro-m4`). This is the **top-level grouping/filter** in the UI — results from different systems are not directly comparable. The UI should:
- Default to showing one system at a time
- Provide a system selector when multiple systems exist in the data
- Clearly label which system's results are being displayed

### Other Key Dimensions

- `suite` — benchmark suite (rtabench, clickbench, time_series, kaggle_airbnb)
- `db` — database under test (duckdb, clickhouse, timescaledb, monetdb, questdb, postgres)
- `operation` — populate or run

## Directory Structure

```
site/
├── public/data/          ← results.duckdb + manifest (from olap publish)
├── src/
│   ├── lib/duckdb.ts     ← DuckDB WASM init + query helpers
│   ├── components/
│   │   ├── QueryTable.tsx ← TanStack Table wrapper
│   │   ├── Chart.tsx      ← Recharts wrappers
│   │   └── filters/       ← system/suite/db/operation selectors
│   ├── App.tsx
│   └── main.tsx
├── index.html
├── vite.config.ts
├── package.json
└── tsconfig.json
```

## Deployment

- `vite build` → static `dist/` directory
- Set `base: '/olap-benchmarks/'` in vite config for GitHub Pages
- GitHub Actions pipeline (to be configured later)
