# Repository Guidelines

## Project Structure & Module Organization
- Application code lives under `src/`, tests under `tests/`.
- Keep data files in `data/` and assets in `assets/` to avoid mixing them with source code.
- Use `docs/` for model references, research files, and user-facing documentation.
- Run outputs are stored under `results/<yyyymmdd-hhmmss>` with logs and Excel exports.
- Raw payloads are stored under `data/<yyyymmdd-hhmmss>` as `*.fundamentals.json`, `*.prices.json`,
  `upcoming-earnings.json`, `upcoming-splits.json`, `upcoming-dividends-YYYY-MM-DD.json`,
  and `exchanges-list.json`.
- Normalized JSON models are stored under `data/<TICKER>.json`.

## Build, Test, and Development Commands
- `python -m venv .venv` creates the local virtual environment.
- `source .venv/bin/activate` (or `.venv\\Scripts\\activate` on Windows) activates the environment.
- `pip install -r requirements.txt` installs pinned dependencies once a requirements file exists.
- `pip install pandas requests pydantic mypy openpyxl toolz more-itertools sqlalchemy psycopg[binary]` installs the current runtime dependencies and type checker.
- `python -m src.app` runs the main module when it is introduced.
- Run pipeline: `python main.py` (requires `EODHD_API_KEY`).
- Optional Postgres persistence via `HARBOUR_BRIDGE_DB_URL`.
- Configure float comparison tolerances in `config.toml`.
- Configure calendar lookahead days in `config.toml` (`calendar.lookahead_days`, capped at 30).
- Use `database.require_db = true` in `config.toml` to require `HARBOUR_BRIDGE_DB_URL`.
- When `HARBOUR_BRIDGE_DB_URL` is set, preflight checks validate DB connectivity and
  run a write/read/delete round-trip on `pipeline_scratch` before downloads.

## Coding Style & Naming Conventions
- Target Python 3.12+ and use strict type hinting throughout the codebase.
- Stongly prefer heavy functional programming style. Always use FP style unless it materially impairs readability. Use native libraries as well as 'toolz' and 'more-itertools'.
- Use 4-space indentation and standard Python conventions (PEP 8).
- Prefer clear module names such as `forecasting.py`, `valuation.py`, and `portfolio.py`.
- If you add formatting or linting, document the tool and command here (e.g., `ruff format`, `black`).
- PyCharm users: enable built-in inspections and keep auto-formatting aligned to PEP 8.

## Testing Guidelines
- Use `pytest` with file names like `test_*.py` in `tests/`.
- Test command: `pytest -q`.
- Database integration tests require `HARBOUR_BRIDGE_DB_URL` and skip when unset.

## Static Analysis & Data Validation
- Use `mypy` in strict mode for static analysis and enforce type hints on public APIs.
- Use `pydantic` models for validated, structured data objects.

## Commit & Pull Request Guidelines
- This checkout has no Git history; use short, imperative commit subjects (e.g., "Add ingestion pipeline").
- Pull requests should include a concise summary, testing notes, and links to any relevant issues or docs.

## Agent-Specific Instructions
- Keep `AGENTS.md` updated as the repository gains structure, tooling, and conventions.
