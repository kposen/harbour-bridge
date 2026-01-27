from __future__ import annotations

"""Tests for end-of-day price ingestion and storage."""

import os
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests  # type: ignore[import-untyped]

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import main
from src.io.database import (
    _iter_price_rows,
    ensure_schema,
    get_latest_price_date,
    write_prices,
)
from src.io.storage import save_price_payload


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


def _unique_symbol(prefix: str) -> str:
    """Build a unique ticker symbol for database tests."""
    return f"{prefix}{uuid.uuid4().hex[:6].upper()}.US"


def _price_entry(
    date_str: str,
    open_value: str = "10",
    high_value: str = "12",
    low_value: str = "9",
    close_value: str = "11",
    adjusted_close_value: str = "10.5",
    volume_value: str = "1000",
) -> dict[str, Any]:
    """Build a minimal price entry payload.

    Args:
        date_str (str): ISO date string for the entry.
        open_value (str): Open price value.
        high_value (str): High price value.
        low_value (str): Low price value.
        close_value (str): Close price value.
        adjusted_close_value (str): Adjusted close value.
        volume_value (str): Volume value.

    Returns:
        dict[str, Any]: Price entry payload.
    """
    return {
        "date": date_str,
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "adjusted_close": adjusted_close_value,
        "volume": volume_value,
    }


def test_get_latest_price_date_parses_dates() -> None:
    """Latest price date should parse from stored date columns.

    Args:
        None

    Returns:
        None: Assertions validate parsing behavior.
    """
    engine = _get_engine()
    symbol = _unique_symbol("AAPL")
    # Insert two dates and ensure the latest is returned.
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO prices (
                    symbol,
                    date,
                    retrieval_date,
                    provider,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume
                )
                VALUES (
                    :symbol,
                    :date,
                    :retrieval_date,
                    :provider,
                    :open,
                    :high,
                    :low,
                    :close,
                    :adjusted_close,
                    :volume
                )
                """
            ),
            [
                {
                    "symbol": symbol,
                    "date": date(2025, 1, 1),
                    "retrieval_date": datetime(2025, 1, 2, tzinfo=UTC),
                    "provider": "EODHD",
                    "open": 10.0,
                    "high": 12.0,
                    "low": 9.0,
                    "close": 11.0,
                    "adjusted_close": 10.5,
                    "volume": 1000.0,
                },
                {
                    "symbol": symbol,
                    "date": date(2025, 2, 1),
                    "retrieval_date": datetime(2025, 2, 2, tzinfo=UTC),
                    "provider": "EODHD",
                    "open": 20.0,
                    "high": 21.0,
                    "low": 19.0,
                    "close": 20.5,
                    "adjusted_close": 20.4,
                    "volume": 2000.0,
                },
            ],
        )
    latest = get_latest_price_date(engine, symbol, "EODHD")
    assert latest == date(2025, 2, 1)


def test_write_prices_dedups_identical_versions() -> None:
    """Duplicate price payloads should be deduped by value.

    Args:
        None

    Returns:
        None: Assertions validate versioning behavior.
    """
    engine = _get_engine()
    symbol = _unique_symbol("AAPL")
    payload = [_price_entry("2025-01-01")]

    first = write_prices(
        engine=engine,
        symbol=symbol,
        provider="EODHD",
        retrieval_date=datetime(2025, 2, 1, tzinfo=UTC),
        raw_data=payload,
    )
    second = write_prices(
        engine=engine,
        symbol=symbol,
        provider="EODHD",
        retrieval_date=datetime(2025, 2, 2, tzinfo=UTC),
        raw_data=payload,
    )

    # The second insert should be skipped because values are identical.
    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM prices WHERE symbol = :symbol AND provider = :provider"),
            {"symbol": symbol, "provider": "EODHD"},
        ).scalar()
    assert first == 1
    assert second == 0
    assert count == 1


def test_write_prices_inserts_on_value_change() -> None:
    """Changed values should insert a new version.

    Args:
        None

    Returns:
        None: Assertions validate versioning behavior.
    """
    engine = _get_engine()
    symbol = _unique_symbol("AAPL")
    payload = [_price_entry("2025-01-01", close_value="11")]
    write_prices(
        engine=engine,
        symbol=symbol,
        provider="EODHD",
        retrieval_date=datetime(2025, 2, 1, tzinfo=UTC),
        raw_data=payload,
    )
    updated_payload = [_price_entry("2025-01-01", close_value="11.5")]
    inserted = write_prices(
        engine=engine,
        symbol=symbol,
        provider="EODHD",
        retrieval_date=datetime(2025, 2, 2, tzinfo=UTC),
        raw_data=updated_payload,
    )

    # Only the changed row should insert as a new version.
    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM prices WHERE symbol = :symbol AND provider = :provider"),
            {"symbol": symbol, "provider": "EODHD"},
        ).scalar()
    assert inserted == 1
    assert count == 2


def test_iter_price_rows_accepts_mapping_payload() -> None:
    """Mapping payloads should be parsed into price rows.

    Args:
        None

    Returns:
        None: Assertions validate payload parsing.
    """
    payload = {
        "0": _price_entry("2025-01-01"),
        "1": _price_entry("2025-01-02", close_value="12"),
    }
    rows = list(
        _iter_price_rows(
            symbol="AAPL.US",
            provider="EODHD",
            retrieval_date=datetime(2025, 2, 1, tzinfo=UTC),
            raw_data=payload,
        )
    )
    # Ensure both entries are parsed and dates are preserved.
    dates = {row["date"] for row in rows}
    assert dates == {date(2025, 1, 1), date(2025, 1, 2)}


def test_fetch_prices_uses_from_param(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetch should include 'from' only when a start date is provided.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest monkeypatch fixture.

    Returns:
        None: Assertions validate request parameter behavior.
    """
    monkeypatch.setenv("EODHD_API_KEY", "test-key")
    captured: dict[str, Any] = {}

    class DummyResponse:
        """Lightweight response stub for request mocking."""

        def __init__(self, text_payload: str, json_payload: object | None = None) -> None:
            """Create a dummy response wrapper.

            Args:
                text_payload (str): Payload to return from text.
                json_payload (object | None): Optional JSON payload.

            Returns:
                None
            """
            self.text = text_payload
            self._payload = json_payload

        def raise_for_status(self) -> None:
            """No-op status check for the dummy response.

            Args:
                None

            Returns:
                None
            """

        def json(self) -> object:
            """Return the payload for the dummy response.

            Args:
                None

            Returns:
                object: Payload for the response.
            """
            if self._payload is None:
                raise ValueError("No JSON payload")
            return self._payload

    def fake_get(url: str, params: dict[str, str], timeout: int) -> DummyResponse:
        """Capture request arguments and return a dummy response.

        Args:
            url (str): URL requested by the client.
            params (dict[str, str]): Request parameters.
            timeout (int): Timeout used for the request.

        Returns:
            DummyResponse: Stubbed response payload.
        """
        captured["url"] = url
        captured["params"] = dict(params)
        captured["timeout"] = timeout
        return DummyResponse("Date,Open,High,Low,Close,Adjusted_close,Volume\n")

    monkeypatch.setattr(requests, "get", fake_get)

    # Without a start date, we should not include "from".
    payload = main.fetch_prices("AAPL.US", None)
    assert payload == []
    assert "from" not in captured["params"]
    assert captured["params"].get("fmt") == "csv"

    # With a start date, include "from".
    start = date(2025, 1, 15)
    payload = main.fetch_prices("AAPL.US", start)
    assert payload == []
    assert captured["params"].get("from") == "2025-01-15"
    assert captured["params"].get("fmt") == "csv"


def test_save_price_payload_writes_expected_file(tmp_path: Path) -> None:
    """Price payloads should be written with the expected filename.

    Args:
        tmp_path (Path): Pytest temporary directory fixture.

    Returns:
        None: Assertions validate filesystem writes.
    """
    payload = "Date,Open,High,Low,Close,Adjusted_close,Volume\n2025-01-01,1,2,0.5,1.5,1.4,100\n"
    path = save_price_payload(tmp_path, "AAPL.US", payload)
    assert path.name == "AAPL.US.prices.csv"
    assert path.exists()


def test_parse_prices_csv_sample() -> None:
    """CSV price payloads should parse into normalized rows."""
    sample_path = Path(__file__).resolve().parents[1] / "data" / "samples" / "MCD.US.prices.csv"
    csv_text = sample_path.read_text(encoding="utf-8")

    rows = main._parse_prices_csv(csv_text)

    assert rows
    first = rows[0]
    assert first.get("date") == "1966-07-05"
    assert "adjusted_close" in first
