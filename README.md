# Financial Analysis Application

This repository builds a small financial model pipeline that ingests provider
fundamentals, normalizes the data, forecasts forward, and persists results to
disk. The core logic is pure and side effects are isolated in the imperative
shell.

## Overview

- `src/logic/historic_builder.py`: Parse raw provider payloads into `LineItems`.
- `src/logic/forecasting.py`: Generate forecasts from historical data.
- `src/io/storage.py`: Serialize/deserialize data to JSON on disk.
- `main.py`: Imperative pipeline composition and network I/O.

## Quick Start

1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set the EODHD API key:
   ```bash
   export EODHD_API_KEY="your_key_here"
   ```
4. Run the download pipeline (provide one or more tickers):
   ```bash
   python main.py download AAPL.US
   ```
5. Run the forecast pipeline from database facts:
   ```bash
   python main.py forecast AAPL.US
   ```
   You can also run both steps together with:
   ```bash
   python main.py all AAPL.US
   ```
   Omitting the command defaults to `all`.

## Configuration

- `EODHD_API_KEY`: Required. Used by `fetch_data` to call the EODHD fundamentals
  endpoint.
- `HARBOUR_BRIDGE_DB_URL`: Required for `download` and `forecast`. Postgres
  connection string (e.g., `postgresql+psycopg://user:pass@localhost:5432/harbour_bridge`).
  Requires the `psycopg` driver (included in `requirements.txt`).
- `calendar.lookahead_days`: Optional. Days to fetch corporate action calendars
  (clamped to 1-30 by the pipeline).
- `universe.refresh_days`: Optional. Share universe refresh cadence in days.
- `prices.max_symbols_for_prices`: Optional. Max symbols to refresh prices per run
  (`-1` means unlimited).
- `prices.days_stale`: Optional. Days since last price update before triggering
  a full-history refresh (default 7).
- Ticker format: `"TICKER.EXCHANGE"` (e.g., `AAPL.US`).
- `config.toml`: Optional. Database float comparison tolerances for deduping.

## Data Flow

1. **Download**: `download` fetches fundamentals, prices, calendars, and exchange lists.
2. **Persist raw payloads**: Stored under `data/<timestamp>` for each run, including
   `*.fundamentals.json`, `*.prices.json`, `upcoming-earnings.json`,
   `upcoming-splits.json`, `upcoming-dividends-YYYY-MM-DD.json`, and
   `exchanges-list.json`.
3. **Database**: Reported facts and price history are written to Postgres.
4. **Forecast**: `forecast` loads reported facts from Postgres, runs
   `generate_forecast`, and exports reports.
5. **Outputs**: Excel exports and debug logs are written to `results/<timestamp>`.

## Database Storage (Optional)

If you want to persist normalized facts to Postgres, apply the schema in
`docs/sql/schema.sql`, set `HARBOUR_BRIDGE_DB_URL`, and use the helpers in `src/io/database.py` to insert rows
into `financial_facts` and `prices`. The primary key for `financial_facts`
includes symbol, fiscal date, filing date, retrieval date, period type,
statement, line item, and value source to preserve versions and reported vs
calculated values. The primary key for `prices` includes symbol, date,
retrieval_date, and provider for versioned price history.

The `download` and `forecast` commands run preflight checks before accessing
Postgres. This validates connectivity and performs a write/read/delete round-trip
against a scratch table named `pipeline_scratch`. Failures abort the run.
The exchange list is stored in `exchanges` with explicit columns for
`name`, `operating_mic`, `country`, `currency`, `country_iso2`, and
`country_iso3`.
Symbol integrity events are stored in `symbol_integrity` for auditability
(success, failures, skips).

## Notes

- Only annual financials are currently used for forecasting.
- The pipeline always calls the network; caching will be added later.
- The forecasting logic uses simple averages as a placeholder.

## Testing

Run tests with:
```bash
pytest -q
```

Database integration tests require `HARBOUR_BRIDGE_DB_URL` to be set and will
be skipped when it is missing.

## Repository Structure

```
docs/            Reference documents and payload samples
src/             Application logic
tests/           Pytest suite
data/            Stored JSON outputs (created at runtime)
```
