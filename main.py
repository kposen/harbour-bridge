from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

import requests

from src.domain.schemas import Assumptions
from src.io.database import (
    ensure_schema,
    get_engine,
    get_latest_filing_date,
    get_latest_price_date,
    write_holders,
    write_financial_facts,
    write_earnings,
    write_insider_transactions,
    write_listings,
    write_market_metrics,
    write_prices,
    write_reported_facts,
)
from src.io.reporting import export_model_to_excel
from src.io.storage import (
    build_run_data_dir,
    save_price_payload,
    save_raw_payload,
    save_share_data,
)
from src.logic.forecasting import generate_forecast
from src.logic.historic_builder import build_historic_model
from src.logic.validation import validate_eodhd_payload


logger = logging.getLogger(__name__)


def get_tickers_needing_update() -> list[str]:
    """Return tickers that should be refreshed by the pipeline.

    Args:
        None

    Returns:
        list[str]: Ticker symbols requiring updates.
    """
    # Allow explicit CLI args; otherwise fall back to the placeholder.
    cli_tickers = list(filter(None, map(str.strip, sys.argv[1:])))
    if cli_tickers:
        return cli_tickers
    # Placeholder: wire this to a watchlist or datastore later.
    return []


def fetch_data(ticker: str) -> dict[str, Any] | None:
    """Fetch raw provider data for a ticker (network I/O happens here).

    Args:
        ticker (str): The ticker symbol to fetch.

    Returns:
        dict[str, Any]: Raw provider payload for the ticker.
    """
    # Keep side effects in this shell to preserve pure core logic.
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    logger.info("Fetching fundamentals for %s", ticker)
    try:
        response = requests.get(
            f"https://eodhd.com/api/fundamentals/{ticker}",
            params={"api_token": api_key, "fmt": "json"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.info("API request failed for %s: %s", ticker, exc)
        return None
    except ValueError as exc:
        logger.info("Failed to decode JSON for %s: %s", ticker, exc)
        return None
    if not isinstance(payload, dict):
        logger.info("EODHD response did not return a JSON object for %s", ticker)
        return None
    if any(key in payload for key in ("Error", "error", "message")) and "Financials" not in payload:
        logger.info("EODHD error payload for %s: %s", ticker, payload)
        return None
    logger.debug("Received fundamentals payload keys: %s", sorted(payload.keys()))
    return payload


def fetch_prices(ticker: str, start_date: date | None) -> object | None:
    """Fetch end-of-day prices for a ticker (network I/O happens here).

    Args:
        ticker (str): The ticker symbol to fetch.
        start_date (date | None): Start date for the request, or None for full history.

    Returns:
        object | None: Raw provider payload for prices, or None on error.
    """
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    params: dict[str, str] = {"api_token": api_key, "fmt": "json"}
    if start_date is not None:
        params["from"] = start_date.isoformat()
    logger.info("Fetching prices for %s", ticker)
    try:
        response = requests.get(
            f"https://eodhd.com/api/eod/{ticker}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.info("Price API request failed for %s: %s", ticker, exc)
        return None
    except ValueError as exc:
        logger.info("Failed to decode price JSON for %s: %s", ticker, exc)
        return None
    if isinstance(payload, dict) and any(key in payload for key in ("Error", "error", "message")):
        logger.info("EODHD price error payload for %s: %s", ticker, payload)
        return None
    if not isinstance(payload, (list, dict)):
        logger.info("EODHD prices response did not return JSON rows for %s", ticker)
        return None
    return payload


def run_pipeline(results_dir: Path) -> None:
    """Run the imperative pipeline: fetch -> build history -> forecast -> save.

    Args:
        results_dir (Path): Directory for run outputs.

    Returns:
        None: Side effects are persisted to storage.
    """
    # Keep assumptions in the shell so logic modules stay pure.
    assumptions = Assumptions(growth_rates={}, margins={})
    tickers = get_tickers_needing_update()
    if tickers:
        logger.info("Loaded %d tickers to evaluate", len(tickers))
        logger.debug("Candidate tickers: %s", tickers)
    else:
        logger.info("No tickers provided; pipeline will exit after setup")
    data_dir = build_run_data_dir(results_dir.name)
    logger.info("Created data directory: %s", data_dir)
    database_url = os.getenv("HARBOUR_BRIDGE_DB_URL")
    engine = get_engine(database_url) if database_url else None
    if engine is None:
        logger.info("HARBOUR_BRIDGE_DB_URL not set; skipping database writes")
    else:
        logger.info("Using Postgres database connection from HARBOUR_BRIDGE_DB_URL")
        ensure_schema(engine)
    tickers_to_process = _filter_stale_tickers(tickers, engine)
    if not tickers_to_process:
        logger.info("No tickers scheduled for update; pipeline complete")
        return
    logger.info("Starting pipeline for %d tickers", len(tickers_to_process))
    logger.debug("Tickers scheduled for update: %s", tickers_to_process)
    provider = "EODHD"
    for ticker in tickers_to_process:
        logger.info("Processing ticker: %s", ticker)
        # Pull raw data, then build a clean historical model.
        retrieval_date = datetime.now(UTC)
        price_start = _price_start_date(engine, ticker, provider)
        if price_start is None:
            logger.debug("No stored price history for %s; fetching full history", ticker)
        else:
            logger.debug("Fetching prices for %s starting from %s", ticker, price_start)
        price_payload = fetch_prices(ticker, price_start)
        if price_payload is not None:
            if isinstance(price_payload, list):
                if not price_payload:
                    logger.info("No new price data returned for %s", ticker)
                else:
                    logger.debug("Received %d price rows for %s", len(price_payload), ticker)
            elif isinstance(price_payload, dict):
                logger.debug(
                    "Received price payload keys for %s: %s",
                    ticker,
                    list(price_payload.keys()),
                )
            save_price_payload(data_dir, ticker, price_payload)
            if engine is not None:
                write_prices(
                    engine=engine,
                    symbol=ticker,
                    provider=provider,
                    retrieval_date=retrieval_date,
                    raw_data=price_payload,
                )
        else:
            logger.info("Skipping price persistence for %s due to fetch error", ticker)
        raw_data = fetch_data(ticker)
        if raw_data is None:
            logger.info("Skipping %s due to fetch error", ticker)
            continue
        warnings = validate_eodhd_payload(raw_data)
        if warnings:
            logger.info("Payload validation warnings for %s: %s", ticker, warnings)
        if "Financials" not in raw_data:
            logger.info("Skipping %s due to missing Financials section", ticker)
            continue
        save_raw_payload(data_dir, ticker, raw_data)
        filing_dates = _extract_filing_dates(raw_data)
        logger.debug("Extracted %d filing dates for %s", len(filing_dates), ticker)
        try:
            historic_model = build_historic_model(raw_data)
        except ValueError as exc:
            logger.info("Skipping %s due to parsing error: %s", ticker, exc)
            continue
        # Add a forecast using placeholder assumptions.
        forecast_model = generate_forecast(historic_model, assumptions)
        # Persist the result as JSON.
        save_share_data(ticker, forecast_model)
        # Write an Excel workbook for each share.
        report_path = results_dir / f"{ticker}.xlsx"
        export_model_to_excel(forecast_model, report_path)
        logger.info("Wrote report to %s", report_path)
        # Persist to Postgres when configured.
        if engine is not None:
            write_market_metrics(
                engine=engine,
                symbol=ticker,
                retrieval_date=retrieval_date,
                raw_data=raw_data,
            )
            write_earnings(
                engine=engine,
                symbol=ticker,
                retrieval_date=retrieval_date,
                raw_data=raw_data,
            )
            write_holders(
                engine=engine,
                symbol=ticker,
                retrieval_date=retrieval_date,
                raw_data=raw_data,
            )
            write_insider_transactions(
                engine=engine,
                symbol=ticker,
                retrieval_date=retrieval_date,
                raw_data=raw_data,
            )
            write_listings(
                engine=engine,
                retrieval_date=retrieval_date,
                raw_data=raw_data,
            )
            write_reported_facts(
                engine=engine,
                symbol=ticker,
                provider=provider,
                retrieval_date=retrieval_date,
                raw_data=raw_data,
            )
            write_financial_facts(
                engine=engine,
                symbol=ticker,
                provider=provider,
                retrieval_date=retrieval_date,
                model=forecast_model,
                filing_dates=filing_dates,
                period_type="annual",
                value_source="calculated",
            )
    logger.info("Pipeline complete")


def _ensure_base_directories() -> tuple[Path, Path, bool, bool]:
    """Ensure the root data/results directories exist.

    Args:
        None

    Returns:
        tuple[Path, Path, bool, bool]: Data path, results path, and creation flags.
    """
    root = Path(__file__).resolve().parent
    data_root = root / "data"
    results_root = root / "results"
    data_created = not data_root.exists()
    results_created = not results_root.exists()
    data_root.mkdir(parents=True, exist_ok=True)
    results_root.mkdir(parents=True, exist_ok=True)
    return data_root, results_root, data_created, results_created


def _build_results_dir(results_root: Path) -> Path:
    """Create a timestamped results directory for the current run.

    Args:
        results_root (Path): Base directory for run outputs.

    Returns:
        Path: Directory path for this run's outputs.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = results_root / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Created results directory: %s", run_dir)
    return run_dir


def _filter_stale_tickers(tickers: list[str], engine) -> list[str]:
    """Filter tickers to those needing updates based on filing date age.

    Args:
        tickers (list[str]): Candidate tickers to process.
        engine (Engine | None): SQL engine when available.

    Returns:
        list[str]: Tickers requiring refresh.
    """
    if not tickers:
        logger.info("No tickers supplied for staleness check")
        return []
    if engine is None:
        logger.info("No database configured; skipping staleness check for %d tickers", len(tickers))
        return tickers
    today = datetime.now(UTC).date()
    cutoff = _months_ago(today, 3)
    logger.debug("Using staleness cutoff date: %s", cutoff)
    should_update = partial(_should_update, engine=engine, cutoff=cutoff)
    stale_tickers = [ticker for ticker in tickers if should_update(ticker)]
    logger.info(
        "Staleness check complete: %d of %d tickers need updates",
        len(stale_tickers),
        len(tickers),
    )
    if not stale_tickers:
        logger.info("All tickers are up to date; nothing to download")
    else:
        logger.debug("Tickers scheduled for update: %s", stale_tickers)
    return stale_tickers


def _should_update(ticker: str, engine, cutoff: date) -> bool:
    """Return True when a ticker should be refreshed.

    Args:
        ticker (str): Ticker symbol to check.
        engine (Engine): SQL engine for queries.
        cutoff (date): Cutoff date for staleness.

    Returns:
        bool: True when the ticker should be refreshed.
    """
    latest_filing = get_latest_filing_date(engine, ticker)
    if latest_filing is None:
        logger.info("No filing date found for %s; scheduling update", ticker)
        return True
    if latest_filing <= cutoff:
        logger.info("Filing date for %s is older than %s; scheduling update", ticker, cutoff)
        return True
    logger.debug("Filing date for %s is current (%s); skipping update", ticker, latest_filing)
    return False


def _price_start_date(engine, ticker: str, provider: str) -> date | None:
    """Return the start date for price downloads.

    Args:
        engine (Engine | None): SQL engine when available.
        ticker (str): Ticker symbol to query.
        provider (str): Provider name (e.g., "EODHD").

    Returns:
        date | None: Latest stored price date, or None for full history.
    """
    if engine is None:
        return None
    return get_latest_price_date(engine, ticker, provider)


def _months_ago(current: date, months: int) -> date:
    """Return the date that is a number of calendar months before current.

    Args:
        current (date): Reference date.
        months (int): Number of months to subtract.

    Returns:
        date: Date months before current, clamped to month end when needed.
    """
    year_offset, month_index = divmod(current.month - months - 1, 12)
    year = current.year + year_offset
    month = month_index + 1
    day = min(current.day, _month_end_day(year, month))
    return date(year, month, day)


def _month_end_day(year: int, month: int) -> int:
    """Return the last day of a given month.

    Args:
        year (int): Year to check.
        month (int): Month to check.

    Returns:
        int: Last day of the month.
    """
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def _extract_filing_dates(raw_data: dict[str, Any]) -> dict[date, date]:
    """Extract filing dates from an EODHD payload keyed by fiscal date.

    Args:
        raw_data (dict[str, Any]): Raw provider payload.

    Returns:
        dict[date, date]: Mapping from fiscal date to filing date.
    """
    financials = raw_data.get("Financials")
    if not isinstance(financials, dict):
        return {}
    pairs = (
        (fiscal_date, filing_date)
        for statement_key in ("Income_Statement", "Balance_Sheet", "Cash_Flow")
        for statement in [financials.get(statement_key)]
        if isinstance(statement, dict)
        for yearly in [statement.get("yearly")]
        if isinstance(yearly, dict)
        for fiscal_str, values in yearly.items()
        if isinstance(values, dict)
        for fiscal_date in [_parse_date(fiscal_str)]
        if fiscal_date is not None
        for filing_date in [_parse_date(values.get("filing_date"))]
        if filing_date is not None
    )
    return dict(pairs)


def _parse_date(value: object) -> date | None:
    """Parse a date from an ISO string.

    Args:
        value (object): Value to parse.

    Returns:
        date | None: Parsed date if possible.
    """
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


if __name__ == "__main__":
    data_root, results_root, data_created, results_created = _ensure_base_directories()
    results_dir = _build_results_dir(results_root)
    log_path = results_dir / "run.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[console_handler, file_handler])
    if data_created:
        logger.info("Created data directory: %s", data_root)
    else:
        logger.info("Using existing data directory: %s", data_root)
    if results_created:
        logger.info("Created results directory: %s", results_root)
    else:
        logger.info("Using existing results directory: %s", results_root)
    logger.info("Run output directory: %s", results_dir)
    run_pipeline(results_dir)
