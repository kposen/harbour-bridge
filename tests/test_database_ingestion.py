from __future__ import annotations

"""Tests for database ingestion helpers and staleness logic."""

from datetime import UTC, date, datetime

from sqlalchemy import create_engine, text

import main
from src.io.database import _iter_reported_rows, ensure_schema, get_latest_filing_date


def test_staleness_logic_with_date_columns() -> None:
    """Staleness logic should parse stored dates from the database.

    Args:
        None

    Returns:
        None: Assertions validate staleness behavior.
    """
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)
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
                "symbol": "TEST.US",
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

    latest = get_latest_filing_date(engine, "TEST.US")
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
        stale = main._filter_stale_tickers(["TEST.US"], engine)
        assert stale == ["TEST.US"]
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
