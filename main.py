from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from math import isclose
from typing import Any, Callable, Iterable, Mapping

import requests
from tqdm import tqdm
from sqlalchemy.engine import Engine
from src.config import (
    get_calendar_lookahead_days,
    get_database_tolerances,
    get_universe_refresh_days,
)

from src.domain.schemas import Assumptions, FinancialModel
from src.io.database import (
    ensure_schema,
    get_engine,
    get_latest_filing_date,
    get_latest_price_date,
    get_price_day_snapshot,
    get_symbols_with_history,
    get_filtered_universe_symbols,
    get_exchange_codes,
    get_unmatched_open_refreshes,
    load_historic_model_from_db,
    run_database_preflight,
    append_refresh_schedule_row,
    write_corporate_actions_calendar,
    write_bulk_dividends,
    write_bulk_splits,
    write_price_history,
    parse_price_history_csv,
    write_exchange_list,
    write_holders,
    write_financial_facts,
    write_insider_transactions,
    write_listings,
    write_market_metrics,
    write_reported_facts,
    write_share_universe,
)
from src.io.reporting import export_model_to_excel
from src.io.storage import (
    build_run_data_dir,
    save_exchanges_list_payload,
    save_exchange_shares_payload,
    save_upcoming_dividends_payload,
    save_bulk_dividends_payload,
    save_bulk_splits_payload,
    save_price_history_payload,
    save_upcoming_earnings_payload,
    save_upcoming_splits_payload,
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
    entries: list[object] = []
    next_url: str | None = "https://eodhd.com/api/calendar/dividends"
    next_params: dict[str, str] | None = params
    seen_urls: set[str] = set()

    def _extract_entries(page_payload: object) -> list[object]:
        if isinstance(page_payload, list):
            return list(page_payload)
        if isinstance(page_payload, dict):
            data = page_payload.get("data")
            if isinstance(data, list):
                return list(data)
            dividends = page_payload.get("dividends")
            if isinstance(dividends, list):
                return list(dividends)
            return [entry for entry in page_payload.values() if isinstance(entry, Mapping)]
        return []

    while next_url:
        try:
            response = requests.get(next_url, params=next_params, timeout=30)
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
            logger.info(
                "EODHD dividends calendar response did not return JSON rows for %s",
                payload_date,
            )
            return None
        entries.extend(_extract_entries(payload))
        next_params = None
        next_link: object | None = None
        if isinstance(payload, dict):
            links = payload.get("links")
            if isinstance(links, Mapping):
                next_link = links.get("next")
            else:
                next_link = payload.get("next")
        if next_link is None or str(next_link).strip().lower() in {"", "null"}:
            next_url = None
        elif isinstance(next_link, str):
            if next_link in seen_urls:
                logger.warning("Detected repeated dividends pagination link; stopping at %s", next_link)
                next_url = None
            else:
                seen_urls.add(next_link)
                next_url = next_link
        else:
            next_url = None
    logger.debug("Received %d dividends calendar entries for %s", len(entries), payload_date)
    return entries


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


def fetch_bulk_dividends(exchange_code: str, payload_date: date) -> str | None:
    """Fetch bulk dividends CSV for a specific exchange and date."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    normalized = exchange_code.strip().upper()
    if not normalized:
        logger.info("Exchange code is empty; skipping bulk dividends request")
        return None
    logger.debug("Fetching bulk dividends for %s on %s", normalized, payload_date)
    try:
        response = requests.get(
            f"https://eodhd.com/api/eod-bulk-last-day/{normalized}",
            params={
                "api_token": api_key,
                "date": payload_date.isoformat(),
                "type": "dividends",
                "fmt": "csv",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        logger.warning("Bulk dividends request failed for %s: %s", normalized, exc)
        return None


def fetch_bulk_splits(exchange_code: str, payload_date: date) -> str | None:
    """Fetch bulk splits CSV for a specific exchange and date."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    normalized = exchange_code.strip().upper()
    if not normalized:
        logger.info("Exchange code is empty; skipping bulk splits request")
        return None
    logger.debug("Fetching bulk splits for %s on %s", normalized, payload_date)
    try:
        response = requests.get(
            f"https://eodhd.com/api/eod-bulk-last-day/{normalized}",
            params={
                "api_token": api_key,
                "date": payload_date.isoformat(),
                "type": "splits",
                "fmt": "csv",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        logger.warning("Bulk splits request failed for %s: %s", normalized, exc)
        return None


def fetch_price_history(symbol: str, start_date: date | None = None) -> str | None:
    """Fetch price history CSV for a symbol."""
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    normalized = symbol.strip().upper()
    if not normalized:
        logger.info("Symbol is empty; skipping price history request")
        return None
    params = {"api_token": api_key, "fmt": "csv"}
    if start_date is not None:
        params["from"] = start_date.isoformat()
    logger.debug("Fetching price history for %s", normalized)
    try:
        response = requests.get(
            f"https://eodhd.com/api/eod/{normalized}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        logger.warning("Price history request failed for %s: %s", normalized, exc)
        return None


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
    run_retrieval: datetime | None = None,
) -> list[str]:
    """Download data and populate the database."""
    logger.info("Starting download pipeline")
    if engine is None:
        engine = _init_engine(database_required=True)
        if engine is None:
            raise RuntimeError("Database engine is required for download pipeline")
    data_dir = build_run_data_dir(results_dir.name)
    logger.info("Created data directory: %s", data_dir)
    if tickers:
        logger.info("Loaded %d tickers to evaluate", len(tickers))
        logger.debug("Candidate tickers: %s", tickers)
    else:
        logger.info("No tickers provided; download pipeline will only refresh calendars")
    if run_retrieval is None:
        run_retrieval = datetime.now(UTC)
    calendar_start = run_retrieval.date()
    calendar_lookahead = get_calendar_lookahead_days()
    logger.debug("Calendar look-ahead requested: %d days", calendar_lookahead)
    if calendar_lookahead < 1:
        logger.warning(
            "Calendar look-ahead of %d days is invalid; using 1",
            calendar_lookahead,
        )
        calendar_lookahead = 1
    elif calendar_lookahead > 30:
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
    refresh_today = run_retrieval.date()
    unmatched_refreshes = get_unmatched_open_refreshes(engine, pipeline="universe")
    due_refreshes = _due_refresh_records(unmatched_refreshes, refresh_today)
    if not unmatched_refreshes:
        inception_index = append_refresh_schedule_row(
            engine=engine,
            open_index=None,
            pipeline="universe",
            cause="inception",
            retrieval_date=run_retrieval,
            refresh_date=refresh_today,
            status="opened",
        )
        due_refreshes = [
            {"index": inception_index, "pipeline": "universe", "cause": "inception"}
        ]
        logger.info("Created inception share universe refresh at index %d", inception_index)
    if not due_refreshes:
        logger.info("No share universe refresh scheduled for %s; skipping universe update", refresh_today)
    else:
        refresh_ok = True
        try:
            exchange_payload = fetch_exchange_list()
            if exchange_payload is not None:
                save_exchanges_list_payload(data_dir, exchange_payload)
            else:
                refresh_ok = False
                logger.info("Skipping exchanges list payload save due to fetch error")
            exchange_inserted = write_exchange_list(
                engine=engine,
                retrieval_date=run_retrieval,
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
                share_payload = fetch_exchange_share_list(exchange_code)
                if share_payload is not None:
                    save_exchange_shares_payload(data_dir, exchange_code, share_payload)
                    inserted_rows = write_share_universe(
                        engine=engine,
                        retrieval_date=run_retrieval,
                        payload=share_payload,
                    )
                    share_universe_inserted += inserted_rows
                    logger.debug(
                        "Share universe rows inserted for %s: %d",
                        exchange_code,
                        inserted_rows,
                    )
                else:
                    refresh_ok = False
                    logger.info(
                        "Skipping share universe persistence for %s due to fetch error",
                        exchange_code,
                    )
            if share_universe_inserted == 0:
                logger.info("No share universe rows inserted")
        except Exception as exc:
            refresh_ok = False
            logger.exception("Share universe refresh failed: %s", exc)
        if refresh_ok:
            refresh_days = get_universe_refresh_days()
            if refresh_days < 1:
                logger.warning(
                    "Share universe refresh cadence of %d days is invalid; using 1",
                    refresh_days,
                )
                refresh_days = 1
            next_refresh = refresh_today + timedelta(days=refresh_days)
            for record in due_refreshes:
                opened_index = _coerce_int(record.get("index"))
                pipeline = record.get("pipeline")
                cause = record.get("cause")
                if opened_index is None or not isinstance(pipeline, str) or not isinstance(cause, str):
                    logger.warning("Skipping refresh schedule update for invalid record: %s", record)
                    continue
                closed_index = append_refresh_schedule_row(
                    engine=engine,
                    open_index=opened_index,
                    pipeline=pipeline,
                    cause=cause,
                    retrieval_date=run_retrieval,
                    refresh_date=None,
                    status="closed",
                )
                append_refresh_schedule_row(
                    engine=engine,
                    open_index=closed_index,
                    pipeline=pipeline,
                    cause=cause,
                    retrieval_date=run_retrieval,
                    refresh_date=next_refresh,
                    status="opened",
                )
        else:
            failed_refresh_date = refresh_today + timedelta(days=1)
            for record in due_refreshes:
                opened_index = _coerce_int(record.get("index"))
                pipeline = record.get("pipeline")
                cause = record.get("cause")
                if opened_index is None or not isinstance(pipeline, str) or not isinstance(cause, str):
                    logger.warning("Skipping refresh schedule update for invalid record: %s", record)
                    continue
                append_refresh_schedule_row(
                    engine=engine,
                    open_index=opened_index,
                    pipeline=pipeline,
                    cause=cause,
                    retrieval_date=run_retrieval,
                    refresh_date=failed_refresh_date,
                    status="failed",
                )
    _run_bulk_daily_refresh(
        pipeline="bulk_dividends",
        log_label="bulk dividends",
        run_retrieval=run_retrieval,
        data_dir=data_dir,
        engine=engine,
        fetch_payload=fetch_bulk_dividends,
        save_payload=save_bulk_dividends_payload,
        write_payload=write_bulk_dividends,
    )
    _run_bulk_daily_refresh(
        pipeline="bulk_splits",
        log_label="bulk splits",
        run_retrieval=run_retrieval,
        data_dir=data_dir,
        engine=engine,
        fetch_payload=fetch_bulk_splits,
        save_payload=save_bulk_splits_payload,
        write_payload=write_bulk_splits,
    )
    inserted = write_corporate_actions_calendar(
        engine=engine,
        retrieval_date=run_retrieval,
        earnings_payload=upcoming_earnings,
        splits_payload=upcoming_splits,
        dividends_payloads=dividend_payloads,
    )
    if inserted == 0:
        logger.info("No corporate actions calendar rows inserted")
    price_symbols = get_filtered_universe_symbols(engine)
    if not price_symbols:
        logger.info("No symbols available for price history refresh; skipping prices update")
    else:
        logger.info("Starting price history refresh for %d symbols", len(price_symbols))
        price_success = 0
        price_failed = 0
        price_inserted = 0
        for symbol in price_symbols:
            latest_date = get_latest_price_date(engine, symbol)
            if latest_date is None:
                payload = fetch_price_history(symbol, None)
                if payload is None:
                    price_failed += 1
                    continue
                save_price_history_payload(data_dir, symbol, payload)
                rows = parse_price_history_csv(
                    payload=payload,
                    symbol=symbol,
                    provider="EODHD",
                    retrieval_date=run_retrieval,
                )
                price_inserted += write_price_history(engine, rows)
                price_success += 1
                continue
            payload = fetch_price_history(symbol, latest_date)
            if payload is None:
                price_failed += 1
                continue
            save_price_history_payload(data_dir, symbol, payload)
            rows_all = parse_price_history_csv(
                payload=payload,
                symbol=symbol,
                provider="EODHD",
                retrieval_date=run_retrieval,
            )
            overlap_row = next(
                (row for row in rows_all if row.get("date") == latest_date),
                None,
            )
            snapshot = get_price_day_snapshot(engine, symbol, latest_date)
            if overlap_row is None or snapshot is None or not _price_overlap_matches(snapshot, overlap_row):
                logger.warning(
                    "Price overlap mismatch for %s on %s; refreshing full history",
                    symbol,
                    latest_date,
                )
                full_payload = fetch_price_history(symbol, None)
                if full_payload is None:
                    price_failed += 1
                    continue
                save_price_history_payload(data_dir, symbol, full_payload)
                rows_all = parse_price_history_csv(
                    payload=full_payload,
                    symbol=symbol,
                    provider="EODHD",
                    retrieval_date=run_retrieval,
                )
                price_inserted += write_price_history(engine, rows_all)
                price_success += 1
                continue
            rows = [
                row
                for row in rows_all
                if isinstance(row.get("date"), date) and row.get("date") > latest_date
            ]
            price_inserted += write_price_history(engine, rows)
            price_success += 1
        logger.info(
            "Price history summary: symbols=%d successes=%d failures=%d inserted_rows=%d",
            len(price_symbols),
            price_success,
            price_failed,
            price_inserted,
        )
    provider = "EODHD"
    tickers_to_process = _filter_stale_tickers(tickers, engine)
    if not tickers_to_process:
        logger.info("No tickers scheduled for update; skipping ticker processing")
    else:
        logger.info("Starting download processing for %d tickers", len(tickers_to_process))
        logger.debug("Tickers scheduled for update: %s", tickers_to_process)
    for ticker in tickers_to_process:
        logger.info("Processing ticker: %s", ticker)
        retrieval_date = run_retrieval
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
    run_retrieval: datetime | None = None,
) -> None:
    """Generate forecasts using database facts."""
    logger.info("Starting forecast pipeline")
    if engine is None:
        engine = _init_engine(database_required=True)
        if engine is None:
            raise RuntimeError("Database engine is required for forecast pipeline")
    if run_retrieval is None:
        run_retrieval = datetime.now(UTC)
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
            retrieval_date=run_retrieval,
            model=forecast_only_model,
            filing_dates=filing_dates,
            period_type="annual",
            value_source="calculated",
        )
    logger.info("Forecast pipeline complete")


def run_pipeline(results_dir: Path, tickers: list[str]) -> None:
    """Run download and forecast pipelines sequentially."""
    engine = _init_engine(database_required=True)
    run_retrieval = datetime.now(UTC)
    run_download_pipeline(results_dir, tickers, engine=engine, run_retrieval=run_retrieval)
    run_forecast_pipeline(results_dir, tickers, engine=engine, run_retrieval=run_retrieval)


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


def _due_refresh_records(
    open_records: list[dict[str, object]],
    today: date,
) -> list[dict[str, object]]:
    """Filter refresh schedule records to those due for execution."""
    def _as_date(value: object) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return None

    return [
        record
        for record in open_records
        for due_date in [_as_date(record.get("due_date"))]
        if due_date is not None and due_date <= today
    ]


def _cutoff_reached(run_retrieval: datetime) -> bool:
    """Return True when the 10:00 UTC cutoff has been reached."""
    utc_retrieval = (
        run_retrieval if run_retrieval.tzinfo is not None else run_retrieval.replace(tzinfo=UTC)
    ).astimezone(UTC)
    return (utc_retrieval.hour, utc_retrieval.minute, utc_retrieval.second, utc_retrieval.microsecond) >= (
        10,
        0,
        0,
        0,
    )


def _bulk_target_date(run_retrieval: datetime) -> date:
    """Return the bulk payload date based on the 10:00 UTC cutoff."""
    utc_date = (
        run_retrieval if run_retrieval.tzinfo is not None else run_retrieval.replace(tzinfo=UTC)
    ).astimezone(UTC).date()
    days_back = 1 if _cutoff_reached(run_retrieval) else 2
    return utc_date - timedelta(days=days_back)


def _next_cutoff_date(run_retrieval: datetime) -> date:
    """Return the next cutoff date (10:00 UTC) for scheduling."""
    utc_date = run_retrieval.astimezone(UTC).date()
    if _cutoff_reached(run_retrieval):
        return utc_date + timedelta(days=1)
    return utc_date


def _price_overlap_matches(
    existing: Mapping[str, object],
    incoming: Mapping[str, object],
) -> bool:
    """Return True when price overlap rows match on OHLC values."""
    rel_tol, abs_tol = get_database_tolerances()
    for key in ("open", "high", "low", "close"):
        existing_val = existing.get(key)
        incoming_val = incoming.get(key)
        if existing_val is None or incoming_val is None:
            return False
        if not isinstance(existing_val, (int, float)) or not isinstance(incoming_val, (int, float)):
            return False
        if not isclose(float(existing_val), float(incoming_val), rel_tol=rel_tol, abs_tol=abs_tol):
            return False
    return True


def _run_bulk_daily_refresh(
    *,
    pipeline: str,
    log_label: str,
    run_retrieval: datetime,
    data_dir: Path,
    engine: Engine,
    fetch_payload: Callable[[str, date], str | None],
    save_payload: Callable[[Path, str, date, str], Path],
    write_payload: Callable[..., int],
) -> None:
    """Run a daily bulk refresh with refresh_schedule tracking."""
    refresh_today = run_retrieval.date()
    unmatched = get_unmatched_open_refreshes(engine, pipeline=pipeline)
    due_refreshes = _due_refresh_records(unmatched, refresh_today)
    if not unmatched:
        inception_index = append_refresh_schedule_row(
            engine=engine,
            open_index=None,
            pipeline=pipeline,
            cause="inception",
            retrieval_date=run_retrieval,
            refresh_date=refresh_today,
            status="opened",
        )
        due_refreshes = [{"index": inception_index, "pipeline": pipeline, "cause": "inception"}]
        logger.info("Created inception %s refresh at index %d", log_label, inception_index)
    if not due_refreshes:
        logger.info(
            "No %s refresh scheduled for %s; skipping %s update",
            log_label,
            refresh_today,
            log_label,
        )
        return
    refresh_ok = True
    target_date = _bulk_target_date(run_retrieval)
    total_rows = 0
    failure_exchanges = 0
    exchange_codes: list[str] = []
    try:
        exchange_codes = get_exchange_codes(engine)
        if not exchange_codes:
            logger.info("No eligible exchanges found; skipping %s refresh", log_label)
        else:
            logger.info("Refreshing %s for %d exchanges", log_label, len(exchange_codes))
            logger.debug(
                "Exchange codes scheduled for %s refresh (sample): %s",
                log_label,
                exchange_codes[:25],
            )
        for exchange_code in tqdm(
            exchange_codes,
            desc=f"{log_label} exchanges",
            unit="ex",
            leave=False,
        ):
            payload = fetch_payload(exchange_code, target_date)
            if payload is None:
                refresh_ok = False
                failure_exchanges += 1
                continue
            save_payload(data_dir, exchange_code, target_date, payload)
            inserted_rows = write_payload(
                engine=engine,
                retrieval_date=run_retrieval,
                payload=payload,
                target_date=target_date,
            )
            total_rows += inserted_rows
    except Exception as exc:
        refresh_ok = False
        logger.exception("%s refresh failed: %s", log_label, exc)
    logger.info(
        "%s summary: exchanges=%d inserted_rows=%d failures=%d target_date=%s",
        log_label,
        len(exchange_codes),
        total_rows,
        failure_exchanges,
        target_date,
    )
    if refresh_ok:
        next_refresh = _next_cutoff_date(run_retrieval)
        for record in due_refreshes:
            opened_index = _coerce_int(record.get("index"))
            pipeline_value = record.get("pipeline")
            cause = record.get("cause")
            if opened_index is None or not isinstance(pipeline_value, str) or not isinstance(cause, str):
                logger.warning("Skipping refresh schedule update for invalid record: %s", record)
                continue
            closed_index = append_refresh_schedule_row(
                engine=engine,
                open_index=opened_index,
                pipeline=pipeline_value,
                cause=cause,
                retrieval_date=run_retrieval,
                refresh_date=None,
                status="closed",
            )
            append_refresh_schedule_row(
                engine=engine,
                open_index=closed_index,
                pipeline=pipeline_value,
                cause=cause,
                retrieval_date=run_retrieval,
                refresh_date=next_refresh,
                status="opened",
            )
    else:
        failed_refresh_date = refresh_today
        for record in due_refreshes:
            opened_index = _coerce_int(record.get("index"))
            pipeline_value = record.get("pipeline")
            cause = record.get("cause")
            if opened_index is None or not isinstance(pipeline_value, str) or not isinstance(cause, str):
                logger.warning("Skipping refresh schedule update for invalid record: %s", record)
                continue
            append_refresh_schedule_row(
                engine=engine,
                open_index=opened_index,
                pipeline=pipeline_value,
                cause=cause,
                retrieval_date=run_retrieval,
                refresh_date=failed_refresh_date,
                status="failed",
            )


def _coerce_int(value: object) -> int | None:
    """Coerce a value to int with a None fallback."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _filter_stale_tickers(
    tickers: list[str],
    engine: Engine | None,
    current_date: date | None = None,
) -> list[str]:
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
    today = current_date or datetime.now(UTC).date()
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


def _should_update(ticker: str, engine: Engine, cutoff: date) -> bool:
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
