# Development instructions

* Use Python 3.13 with full typing
* Use uv, never pip
* Test code with pyright, ruff and pytest
* Don't write unnecessary code comments or docstrings (docstrings are OK for cyclopts CLI descriptions)
* Don't add compatibility fallbacks when changing interfaces for external services, e.g. the results database
