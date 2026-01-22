# Repository Guidelines

## Project Structure & Module Organization
- Application code lives under `src/`, tests under `tests/`.
- Keep data files in `data/` and assets in `assets/` to avoid mixing them with source code.
- Use `docs/` for model references, research files, and user-facing documentation.
- Run outputs are stored under `results/<yyyymmdd-hhmmss>` with logs and Excel exports.
- Raw payloads are stored under `data/<yyyymmdd-hhmmss>`; normalized JSON models are stored under `data/<TICKER>.json`.

## Build, Test, and Development Commands
- `python -m venv .venv` creates the local virtual environment.
- `source .venv/bin/activate` (or `.venv\\Scripts\\activate` on Windows) activates the environment.
- `pip install -r requirements.txt` installs pinned dependencies once a requirements file exists.
- `pip install pandas requests pydantic mypy openpyxl toolz more-itertools` installs the current runtime dependencies and type checker.
- `python -m src.app` runs the main module when it is introduced.
- Run pipeline: `python main.py` (requires `EODHD_API_KEY`).
- Optional SQLite persistence via `SQLITE_DB_PATH`.
- Configure float comparison tolerances in `config.toml`.

## Coding Style & Naming Conventions
- Target Python 3.12+ and use strict type hinting throughout the codebase.
- Use 4-space indentation and standard Python conventions (PEP 8).
- Prefer clear module names such as `forecasting.py`, `valuation.py`, and `portfolio.py`.
- If you add formatting or linting, document the tool and command here (e.g., `ruff format`, `black`).
- PyCharm users: enable built-in inspections and keep auto-formatting aligned to PEP 8.

## Testing Guidelines
- Use `pytest` with file names like `test_*.py` in `tests/`.
- Test command: `pytest -q`.

## Static Analysis & Data Validation
- Use `mypy` in strict mode for static analysis and enforce type hints on public APIs.
- Use `pydantic` models for validated, structured data objects.

## Commit & Pull Request Guidelines
- This checkout has no Git history; use short, imperative commit subjects (e.g., "Add ingestion pipeline").
- Pull requests should include a concise summary, testing notes, and links to any relevant issues or docs.

## Agent-Specific Instructions
- Keep `AGENTS.md` updated as the repository gains structure, tooling, and conventions.
