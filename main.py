from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Iterable

import requests
from sqlalchemy.engine import Engine

from src.config import get_calendar_lookahead_days

from src.domain.schemas import Assumptions, FinancialModel
from src.io.database import (
    ensure_schema,
    get_engine,
    get_latest_filing_date,
    get_latest_price_date,
    get_symbols_with_history,
    get_exchange_codes,
    load_historic_model_from_db,
    run_database_preflight,
    write_corporate_actions_calendar,
    write_exchange_list,
    write_holders,
    write_financial_facts,
    write_earnings,
    write_insider_transactions,
    write_listings,
    write_market_metrics,
    write_prices,
    write_reported_facts,
    write_share_universe,
)
from src.io.reporting import export_model_to_excel
from src.io.storage import (
    build_run_data_dir,
    save_exchanges_list_payload,
    save_exchange_shares_payload,
    save_upcoming_dividends_payload,
    save_upcoming_earnings_payload,
    save_upcoming_splits_payload,
    save_price_payload,
    save_raw_payload,
    save_share_data,
)
from src.logic.forecasting import generate_forecast
from src.logic.validation import validate_eodhd_payload


logger = logging.getLogger(__name__)


def _normalize_tickers(tickers: Iterable[str]) -> list[str]:
    """Normalize ticker inputs into a list of non-empty strings."""
    return [ticker for ticker in (ticker.strip() for ticker in tickers) if ticker]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI arguments for pipeline commands."""
    parser = argparse.ArgumentParser(description="Harbour Bridge pipeline runner")
    subparsers = parser.add_subparsers(dest="command")
    for command, help_text in (
        ("download", "Download provider data and populate the database."),
        ("forecast", "Generate forecasts from database facts."),
        ("all", "Run download then forecast."),
    ):
        sub = subparsers.add_parser(command, help=help_text)
        sub.add_argument("tickers", nargs="*", help="Tickers to process (e.g., AAPL.US)")
    if not argv:
        argv = ["all"]
    elif argv[0] not in {"download", "forecast", "all"}:
        argv = ["all", *argv]
    return parser.parse_args(argv)


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


def _fetch_calendar(endpoint: str, label: str, start_date: date, end_date: date) -> object | None:
    """Fetch calendar data for a date range from EODHD.

    Args:
        endpoint (str): Calendar endpoint path (e.g., "calendar/earnings").
        label (str): Logging label for the calendar request.
        start_date (date): Start date for the calendar window.
        end_date (date): End date for the calendar window.

    Returns:
        object | None: Calendar payload, or None on error.
    """
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    params = {
        "api_token": api_key,
        "fmt": "json",
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
    }
    logger.info("Fetching upcoming %s calendar from %s to %s", label, start_date, end_date)
    try:
        response = requests.get(
            f"https://eodhd.com/api/{endpoint}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.info("Calendar %s request failed: %s", label, exc)
        return None
    except ValueError as exc:
        logger.info("Failed to decode %s calendar JSON: %s", label, exc)
        return None
    if isinstance(payload, dict) and any(key in payload for key in ("Error", "error", "message")):
        logger.info("EODHD %s calendar error payload: %s", label, payload)
        return None
    if not isinstance(payload, (list, dict)):
        logger.info("EODHD %s calendar response did not return JSON rows", label)
        return None
    if isinstance(payload, list):
        logger.debug("Received %d %s calendar entries", len(payload), label)
    return payload


def fetch_upcoming_earnings(start_date: date, end_date: date) -> object | None:
    """Fetch upcoming earnings reports for a date range."""
    return _fetch_calendar("calendar/earnings", "earnings", start_date, end_date)


def fetch_upcoming_splits(start_date: date, end_date: date) -> object | None:
    """Fetch upcoming splits for a date range."""
    return _fetch_calendar("calendar/splits", "splits", start_date, end_date)


def fetch_upcoming_dividends(payload_date: date) -> object | None:
    """Fetch upcoming dividends for a specific date."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    params = {
        "api_token": api_key,
        "fmt": "json",
        "filter[date_eq]": payload_date.isoformat(),
    }
    logger.info("Fetching upcoming dividends for %s", payload_date)
    try:
        response = requests.get(
            "https://eodhd.com/api/calendar/dividends",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.info("Calendar dividends request failed for %s: %s", payload_date, exc)
        return None
    except ValueError as exc:
        logger.info("Failed to decode dividends calendar JSON for %s: %s", payload_date, exc)
        return None
    if isinstance(payload, dict) and any(key in payload for key in ("Error", "error", "message")):
        logger.info("EODHD dividends calendar error payload for %s: %s", payload_date, payload)
        return None
    if not isinstance(payload, (list, dict)):
        logger.info("EODHD dividends calendar response did not return JSON rows for %s", payload_date)
        return None
    if isinstance(payload, list):
        logger.debug("Received %d dividends calendar entries for %s", len(payload), payload_date)
    return payload


def fetch_exchange_list() -> object | None:
    """Fetch the provider exchange list."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    logger.info("Fetching exchanges list")
    try:
        response = requests.get(
            "https://eodhd.com/api/exchanges-list/",
            params={"api_token": api_key, "fmt": "json"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.info("Exchange list request failed: %s", exc)
        return None
    except ValueError as exc:
        logger.info("Failed to decode exchange list JSON: %s", exc)
        return None
    if isinstance(payload, dict) and any(key in payload for key in ("Error", "error", "message")):
        logger.info("EODHD exchange list error payload: %s", payload)
        return None
    if not isinstance(payload, (list, dict)):
        logger.info("EODHD exchange list response did not return JSON rows")
        return None
    if isinstance(payload, list):
        logger.debug("Received %d exchange list entries", len(payload))
    return payload


def fetch_exchange_share_list(exchange_code: str) -> object | None:
    """Fetch the share universe for a specific exchange code."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    normalized = exchange_code.strip().upper()
    if not normalized:
        logger.info("Exchange code is empty; skipping share universe request")
        return None
    logger.info("Fetching share universe for %s", normalized)
    try:
        response = requests.get(
            f"https://eodhd.com/api/exchange-symbol-list/{normalized}",
            params={"api_token": api_key, "fmt": "json"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.info("Share universe request failed for %s: %s", normalized, exc)
        return None
    except ValueError as exc:
        logger.info("Failed to decode share universe JSON for %s: %s", normalized, exc)
        return None
    if isinstance(payload, dict) and any(key in payload for key in ("Error", "error", "message")):
        logger.info("EODHD share universe error payload for %s: %s", normalized, payload)
        return None
    if not isinstance(payload, (list, dict)):
        logger.info("EODHD share universe response did not return JSON rows for %s", normalized)
        return None
    if isinstance(payload, list):
        logger.debug("Received %d share universe entries for %s", len(payload), normalized)
    return payload


def _init_engine(database_required: bool) -> Engine | None:
    """Initialize a Postgres engine with preflight checks."""
    database_url = os.getenv("HARBOUR_BRIDGE_DB_URL")
    if not database_url:
        if database_required:
            logger.error("HARBOUR_BRIDGE_DB_URL is required but not set; aborting pipeline")
            sys.exit(1)
        logger.info("HARBOUR_BRIDGE_DB_URL not set; skipping database setup")
        return None
    engine = get_engine(database_url)
    logger.info("Using Postgres database connection from HARBOUR_BRIDGE_DB_URL")
    logger.info("Starting preflight checks")
    try:
        ensure_schema(engine)
        run_database_preflight(engine)
    except Exception as exc:
        logger.exception("Database preflight failed; aborting pipeline: %s", exc)
        sys.exit(1)
    logger.info("Preflight checks complete")
    return engine


def run_download_pipeline(
    results_dir: Path,
    tickers: list[str],
    engine: Engine | None = None,
) -> list[str]:
    """Download data and populate the database."""
    logger.info("Starting download pipeline")
    if engine is None:
        engine = _init_engine(database_required=True)
    data_dir = build_run_data_dir(results_dir.name)
    logger.info("Created data directory: %s", data_dir)
    if tickers:
        logger.info("Loaded %d tickers to evaluate", len(tickers))
        logger.debug("Candidate tickers: %s", tickers)
    else:
        logger.info("No tickers provided; download pipeline will only refresh calendars")
    calendar_retrieval = datetime.now(UTC)
    calendar_start = calendar_retrieval.date()
    calendar_lookahead = get_calendar_lookahead_days()
    logger.debug("Calendar look-ahead requested: %d days", calendar_lookahead)
    if calendar_lookahead > 30:
        logger.warning(
            "Calendar look-ahead of %d days exceeds max 30; using 30",
            calendar_lookahead,
        )
        calendar_lookahead = 30
    calendar_end = calendar_start + timedelta(days=calendar_lookahead - 1)
    logger.debug(
        "Calendar window resolved to start=%s end=%s days=%d",
        calendar_start,
        calendar_end,
        calendar_lookahead,
    )
    exchange_payload = fetch_exchange_list()
    if exchange_payload is not None:
        save_exchanges_list_payload(data_dir, exchange_payload)
    else:
        logger.info("Skipping exchanges list payload save due to fetch error")
    upcoming_earnings = fetch_upcoming_earnings(calendar_start, calendar_end)
    if upcoming_earnings is not None:
        save_upcoming_earnings_payload(data_dir, upcoming_earnings)
    else:
        logger.info("Skipping upcoming earnings payload save due to fetch error")
    upcoming_splits = fetch_upcoming_splits(calendar_start, calendar_end)
    if upcoming_splits is not None:
        save_upcoming_splits_payload(data_dir, upcoming_splits)
    else:
        logger.info("Skipping upcoming splits payload save due to fetch error")
    dividend_payloads: list[object] = []
    dividend_nonempty_days = 0
    dividend_total_entries = 0
    for offset in range(calendar_lookahead):
        payload_date = calendar_start + timedelta(days=offset)
        dividend_payload = fetch_upcoming_dividends(payload_date)
        if dividend_payload is not None:
            save_upcoming_dividends_payload(data_dir, payload_date, dividend_payload)
            dividend_payloads.append(dividend_payload)
            if isinstance(dividend_payload, list):
                if dividend_payload:
                    dividend_nonempty_days += 1
                dividend_total_entries += len(dividend_payload)
        else:
            logger.info("Skipping upcoming dividends payload save for %s due to fetch error", payload_date)
    logger.debug(
        "Dividend payload summary: days=%d nonempty_days=%d total_entries=%d",
        calendar_lookahead,
        dividend_nonempty_days,
        dividend_total_entries,
    )
    exchange_inserted = write_exchange_list(
        engine=engine,
        retrieval_date=calendar_retrieval,
        payload=exchange_payload,
    )
    if exchange_inserted == 0:
        logger.info("No exchange list rows inserted")
    exchange_codes = get_exchange_codes(engine)
    if not exchange_codes:
        logger.info("No eligible exchanges found; skipping share universe refresh")
    else:
        logger.info("Refreshing share universe for %d exchanges", len(exchange_codes))
        logger.debug(
            "Exchange codes scheduled for share universe refresh (sample): %s",
            exchange_codes[:25],
        )
    share_universe_inserted = 0
    for exchange_code in exchange_codes:
        share_retrieval = datetime.now(UTC)
        share_payload = fetch_exchange_share_list(exchange_code)
        if share_payload is not None:
            save_exchange_shares_payload(data_dir, exchange_code, share_payload)
            inserted_rows = write_share_universe(
                engine=engine,
                retrieval_date=share_retrieval,
                payload=share_payload,
            )
            share_universe_inserted += inserted_rows
            logger.debug(
                "Share universe rows inserted for %s: %d",
                exchange_code,
                inserted_rows,
            )
        else:
            logger.info(
                "Skipping share universe persistence for %s due to fetch error",
                exchange_code,
            )
    if share_universe_inserted == 0:
        logger.info("No share universe rows inserted")
    inserted = write_corporate_actions_calendar(
        engine=engine,
        retrieval_date=calendar_retrieval,
        earnings_payload=upcoming_earnings,
        splits_payload=upcoming_splits,
        dividends_payloads=dividend_payloads,
    )
    if inserted == 0:
        logger.info("No corporate actions calendar rows inserted")
    tickers_to_process = _filter_stale_tickers(tickers, engine)
    if not tickers_to_process:
        logger.info("No tickers scheduled for update; skipping ticker processing")
    else:
        logger.info("Starting download processing for %d tickers", len(tickers_to_process))
        logger.debug("Tickers scheduled for update: %s", tickers_to_process)
    provider = "EODHD"
    for ticker in tickers_to_process:
        logger.info("Processing ticker: %s", ticker)
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
    logger.info("Download pipeline complete")
    return tickers_to_process


def run_forecast_pipeline(
    results_dir: Path,
    tickers: list[str],
    engine: Engine | None = None,
) -> None:
    """Generate forecasts using database facts."""
    logger.info("Starting forecast pipeline")
    if engine is None:
        engine = _init_engine(database_required=True)
    if not tickers:
        tickers = get_symbols_with_history(engine, provider="EODHD")
        if tickers:
            logger.info("Loaded %d tickers from database history", len(tickers))
            logger.debug("Database tickers: %s", tickers)
        else:
            logger.info("No tickers found in the database for forecasting")
            return
    assumptions = Assumptions(growth_rates={}, margins={})
    provider = "EODHD"
    for ticker in tickers:
        logger.info("Forecasting ticker: %s", ticker)
        historic_model, filing_dates = load_historic_model_from_db(
            engine=engine,
            symbol=ticker,
            provider=provider,
            period_type="annual",
        )
        if not historic_model.history:
            logger.info("No historical facts found for %s; skipping forecast", ticker)
            continue
        logger.debug("Loaded %d historical periods for %s", len(historic_model.history), ticker)
        forecast_model = generate_forecast(historic_model, assumptions)
        logger.debug("Generated %d forecast periods for %s", len(forecast_model.forecast), ticker)
        save_share_data(ticker, forecast_model)
        report_path = results_dir / f"{ticker}.xlsx"
        export_model_to_excel(forecast_model, report_path)
        logger.info("Wrote report to %s", report_path)
        forecast_only_model = FinancialModel(history=[], forecast=forecast_model.forecast)
        write_financial_facts(
            engine=engine,
            symbol=ticker,
            provider=provider,
            retrieval_date=datetime.now(UTC),
            model=forecast_only_model,
            filing_dates=filing_dates,
            period_type="annual",
            value_source="calculated",
        )
    logger.info("Forecast pipeline complete")


def run_pipeline(results_dir: Path, tickers: list[str]) -> None:
    """Run download and forecast pipelines sequentially."""
    engine = _init_engine(database_required=True)
    run_download_pipeline(results_dir, tickers, engine=engine)
    run_forecast_pipeline(results_dir, tickers, engine=engine)


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
    args = _parse_args(sys.argv[1:])
    tickers = _normalize_tickers(getattr(args, "tickers", []))
    if args.command == "download":
        run_download_pipeline(results_dir, tickers)
    elif args.command == "forecast":
        run_forecast_pipeline(results_dir, tickers)
    else:
        run_pipeline(results_dir, tickers)
