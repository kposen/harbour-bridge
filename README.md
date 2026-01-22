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
- `SQLITE_DB_PATH`: Optional. Filesystem path to a SQLite database
  (e.g., `data/financials.sqlite`).
- Ticker format: `"TICKER.EXCHANGE"` (e.g., `AAPL.US`).
- `config.toml`: Optional. Database float comparison tolerances for deduping.

## Data Flow

1. **Fetch**: `fetch_data` calls the EODHD fundamentals endpoint.
2. **Normalize**: `build_historic_model` converts provider fields into
   `LineItems` using an external mapping (`EODHD_FIELD_MAP`).
3. **Forecast**: `generate_forecast` uses averaged margins and growth rates.
4. **Persist**: `save_share_data` writes JSON to `data/<TICKER>.json`.
5. **Outputs**: Excel exports and debug logs are written to `results/<timestamp>`.
6. **Raw payloads**: Stored under `data/<timestamp>` for each run.

## Database Storage (Optional)

If you want to persist normalized facts to SQLite, apply the schema in
`docs/sql/schema.sql` and use the helpers in `src/io/database.py` to insert rows
into `financial_facts`. The primary key includes symbol, fiscal date, filing
date, retrieval date, period type, statement, line item, and value source to
preserve versions and reported vs calculated values.

## Notes

- Only annual financials are currently used for forecasting.
- The pipeline always calls the network; caching will be added later.
- The forecasting logic uses simple averages as a placeholder.

## Testing

Run tests with:
```bash
pytest -q
```

## Repository Structure

```
docs/            Reference documents and payload samples
src/             Application logic
tests/           Pytest suite
data/            Stored JSON outputs (created at runtime)
```
