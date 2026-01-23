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
4. Run the pipeline (provide one or more tickers):
   ```bash
   python main.py AAPL.US
   ```

## Configuration

- `EODHD_API_KEY`: Required. Used by `fetch_data` to call the EODHD fundamentals
  endpoint.
- `HARBOUR_BRIDGE_DB_URL`: Optional. Postgres connection string
  (e.g., `postgresql+psycopg://user:pass@localhost:5432/harbour_bridge`).
  Requires the `psycopg` driver (included in `requirements.txt`).
- Ticker format: `"TICKER.EXCHANGE"` (e.g., `AAPL.US`).
- `config.toml`: Optional. Database float comparison tolerances for deduping.

## Data Flow

1. **Fetch**: `fetch_data` calls the EODHD fundamentals endpoint.
2. **Fetch prices**: `fetch_prices` calls the EODHD end-of-day endpoint and stores raw payloads.
3. **Normalize**: `build_historic_model` converts provider fields into
   `LineItems` using an external mapping (`EODHD_FIELD_MAP`).
4. **Forecast**: `generate_forecast` uses averaged margins and growth rates.
5. **Persist**: `save_share_data` writes JSON to `data/<TICKER>.json`.
6. **Outputs**: Excel exports and debug logs are written to `results/<timestamp>`.
7. **Calendars**: Upcoming earnings, splits, and dividends are fetched each run.
8. **Raw payloads**: Stored under `data/<timestamp>` for each run, including
   `*.fundamentals.json`, `*.prices.json`, `upcoming-earnings.json`,
   `upcoming-splits.json`, and `upcoming-dividends-YYYY-MM-DD.json`.

## Database Storage (Optional)

If you want to persist normalized facts to Postgres, apply the schema in
`docs/sql/schema.sql`, set `HARBOUR_BRIDGE_DB_URL`, and use the helpers in `src/io/database.py` to insert rows
into `financial_facts` and `prices`. The primary key for `financial_facts`
includes symbol, fiscal date, filing date, retrieval date, period type,
statement, line item, and value source to preserve versions and reported vs
calculated values. The primary key for `prices` includes symbol, date,
retrieval_date, and provider for versioned price history.

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
