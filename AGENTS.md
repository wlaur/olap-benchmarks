# Development instructions

* Use Python 3.13 with full typing
* Use uv, never pip
* In `site/`, use Bun for dependency management and script execution (`bun`, `bun run`), not npm
* Test code with pyright, ruff and pytest
* Don't write unnecessary code comments or docstrings (docstrings are OK for cyclopts CLI descriptions)
* Don't add compatibility fallbacks when changing interfaces for external services, e.g. the results database
* Benchmark results are stored in DuckDB database files (with extension .db) that are included in git
* For ad-hoc results DB inspection, use the read-only CLI entrypoint `./.venv/bin/olap results query --revision <revision> "<sql>"` instead of opening `results/*.db` directly
