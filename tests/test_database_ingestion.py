from __future__ import annotations

"""Tests for database ingestion helpers and staleness logic."""

import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import main
from src.io.database import _iter_reported_rows, ensure_schema, get_latest_filing_date


def _get_engine() -> Engine:
    """Return a Postgres engine for integration tests."""
    database_url = os.getenv("HARBOUR_BRIDGE_DB_URL")
    if not database_url:
        pytest.skip("HARBOUR_BRIDGE_DB_URL not set; skipping Postgres integration tests")
    engine = create_engine(database_url, future=True)
    if engine.dialect.name != "postgresql":
        pytest.skip("HARBOUR_BRIDGE_DB_URL is not a Postgres URL")
    ensure_schema(engine)
    return engine


def _unique_symbol(prefix: str) -> str:
    """Build a unique ticker symbol for database tests."""
    return f"{prefix}{uuid.uuid4().hex[:6].upper()}.US"


def test_staleness_logic_with_date_columns() -> None:
    """Staleness logic should parse stored dates from Postgres.

    Args:
        None

    Returns:
        None: Assertions validate staleness behavior.
    """
    engine = _get_engine()
    symbol = _unique_symbol("TEST")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO financial_facts (
                    symbol,
                    fiscal_date,
                    filing_date,
                    retrieval_date,
                    period_type,
                    statement,
                    line_item,
                    value_source,
                    value,
                    is_forecast,
                    provider
                )
                VALUES (
                    :symbol,
                    :fiscal_date,
                    :filing_date,
                    :retrieval_date,
                    :period_type,
                    :statement,
                    :line_item,
                    :value_source,
                    :value,
                    :is_forecast,
                    :provider
                )
                """
            ),
            {
                "symbol": symbol,
                "fiscal_date": date(2024, 12, 31),
                "filing_date": date(2025, 1, 15),
                "retrieval_date": datetime(2025, 2, 1, tzinfo=UTC),
                "period_type": "annual",
                "statement": "income",
                "line_item": "revenue",
                "value_source": "reported",
                "value": 100.0,
                "is_forecast": False,
                "provider": "EODHD",
            },
        )

    latest = get_latest_filing_date(engine, symbol)
    assert latest == date(2025, 1, 15)

    class FixedDatetime(datetime):
        """Fixed datetime for staleness testing."""

        @classmethod
        def now(cls, tz: object | None = None) -> datetime:
            """Return a fixed datetime for deterministic tests.

            Args:
                cls (type[FixedDatetime]): Class reference for the method.

            Returns:
                datetime: Fixed UTC datetime for staleness logic.
            """
            if tz is None:
                return datetime(2025, 5, 1)
            return datetime(2025, 5, 1, tzinfo=tz)

    original_datetime = main.datetime
    try:
        main.datetime = FixedDatetime  # type: ignore[assignment]
        stale = main._filter_stale_tickers([symbol], engine)
        assert stale == [symbol]
    finally:
        main.datetime = original_datetime


def test_reported_facts_ingestion_net_income_cfs() -> None:
    """Reported cash-flow net income should use line_item 'net_income'.

    Args:
        None

    Returns:
        None: Assertions validate reported fact rows.
    """
    raw_data = {
        "Financials": {
            "Income_Statement": {
                "yearly": {
                    "2024-12-31": {
                        "netIncome": "120",
                        "totalRevenue": "500",
                        "filing_date": "2025-02-15",
                    }
                }
            },
            "Balance_Sheet": {"yearly": {"2024-12-31": {"totalAssets": "900"}}},
            "Cash_Flow": {
                "yearly": {
                    "2024-12-31": {
                        "netIncome": "120",
                        "totalCashFromOperatingActivities": "150",
                        "customField": "42",
                        "filing_date": "2025-02-15",
                    }
                }
            },
        }
    }
    rows = list(
        _iter_reported_rows(
            symbol="TEST.US",
            provider="EODHD",
            retrieval_date=datetime(2025, 3, 1, tzinfo=UTC),
            raw_data=raw_data,
        )
    )
    cfs_net_income = next(
        (
            row
            for row in rows
            if row["statement"] == "cash_flow" and row["line_item"] == "net_income"
        ),
        None,
    )
    assert cfs_net_income is not None
    raw_row = next(
        (
            row
            for row in rows
            if row["statement"] == "cash_flow"
            and row["line_item"] == "customField"
            and row["value_source"] == "reported_raw"
        ),
        None,
    )
    assert raw_row is not None
