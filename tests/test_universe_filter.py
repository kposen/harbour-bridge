from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.io.database import ensure_schema, get_filtered_universe_symbols


def _get_engine() -> Engine:
    """Return a Postgres engine for integration tests."""
    database_url = os.getenv("HARBOUR_BRIDGE_DB_URL")
    if not database_url:
        pytest.skip("HARBOUR_BRIDGE_DB_URL not set; skipping Postgres integration tests")
    engine = create_engine(database_url, future=True)
    if engine.dialect.name != "postgresql":
        pytest.skip("HARBOUR_BRIDGE_DB_URL is not a Postgres URL")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"HARBOUR_BRIDGE_DB_URL unavailable; skipping Postgres tests: {exc}")
    ensure_schema(engine)
    return engine


def _unique_symbol(prefix: str, exchange: str) -> tuple[str, str]:
    """Build unique symbol/code pairs."""
    code = f"{prefix}{uuid.uuid4().hex[:6].upper()}"
    return f"{code}.{exchange}", code


def test_get_filtered_universe_symbols() -> None:
    """Universe filter should include only allowed symbol types."""
    engine = _get_engine()
    now = datetime(2025, 1, 2, tzinfo=UTC)
    earlier = now - timedelta(days=1)

    cur_symbol, cur_code = _unique_symbol("CUR", "FOREX")
    stock_symbol, stock_code = _unique_symbol("STK", "FOREX")
    noisin_symbol, noisin_code = _unique_symbol("NOI", "FOREX")
    other_symbol, other_code = _unique_symbol("ETF", "FOREX")
    latest_symbol, latest_code = _unique_symbol("LAT", "FOREX")
    nyse_symbol, nyse_code = _unique_symbol("NYC", "NYSE")

    insert_sql = text(
        """
        INSERT INTO universe (
            symbol,
            code,
            exchange,
            type,
            isin,
            retrieval_date
        )
        VALUES (
            :symbol,
            :code,
            :exchange,
            :type,
            :isin,
            :retrieval_date
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "symbol": cur_symbol,
                "code": cur_code,
                "exchange": "FOREX",
                "type": "Currency",
                "isin": None,
                "retrieval_date": now,
            },
        )
        conn.execute(
            insert_sql,
            {
                "symbol": stock_symbol,
                "code": stock_code,
                "exchange": "FOREX",
                "type": "Common Stock",
                "isin": "US1234567890",
                "retrieval_date": now,
            },
        )
        conn.execute(
            insert_sql,
            {
                "symbol": noisin_symbol,
                "code": noisin_code,
                "exchange": "FOREX",
                "type": "Common Stock",
                "isin": None,
                "retrieval_date": now,
            },
        )
        conn.execute(
            insert_sql,
            {
                "symbol": other_symbol,
                "code": other_code,
                "exchange": "FOREX",
                "type": "ETF",
                "isin": "US0987654321",
                "retrieval_date": now,
            },
        )
        conn.execute(
            insert_sql,
            {
                "symbol": latest_symbol,
                "code": latest_code,
                "exchange": "FOREX",
                "type": "Common Stock",
                "isin": "US2222222222",
                "retrieval_date": earlier,
            },
        )
        conn.execute(
            insert_sql,
            {
                "symbol": latest_symbol,
                "code": latest_code,
                "exchange": "FOREX",
                "type": "ETF",
                "isin": "US3333333333",
                "retrieval_date": now,
            },
        )
        conn.execute(
            insert_sql,
            {
                "symbol": nyse_symbol,
                "code": nyse_code,
                "exchange": "NYSE",
                "type": "Currency",
                "isin": None,
                "retrieval_date": now,
            },
        )

    forex_symbols = set(get_filtered_universe_symbols(engine, exchange="FOREX"))
    all_symbols = set(get_filtered_universe_symbols(engine))

    assert cur_symbol in forex_symbols
    assert stock_symbol in forex_symbols
    assert noisin_symbol not in forex_symbols
    assert other_symbol not in forex_symbols
    assert latest_symbol not in forex_symbols
    assert nyse_symbol not in forex_symbols
    assert nyse_symbol in all_symbols
