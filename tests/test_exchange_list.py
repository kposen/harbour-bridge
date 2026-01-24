from __future__ import annotations

"""Tests for exchange list parsing and storage."""

import json
from datetime import UTC, datetime
from pathlib import Path

from src.io.database import EXCHANGE_LIST_COLUMNS, _exchange_rows


def _load_exchange_sample() -> list[dict[str, object]]:
    """Load the sample exchanges payload from disk."""
    payload_path = Path(__file__).resolve().parents[1] / "data" / "samples" / "exchanges.json"
    return json.loads(payload_path.read_text(encoding="utf-8"))


def test_exchange_rows_match_sample_payload() -> None:
    """Sample exchanges payload should map into static columns."""
    payload = _load_exchange_sample()
    retrieval_date = datetime(2026, 1, 1, tzinfo=UTC)
    rows = _exchange_rows(retrieval_date, payload)
    expected_keys = {"retrieval_date", "code", *EXCHANGE_LIST_COLUMNS}

    assert len(rows) == 73
    assert all(set(row.keys()) == expected_keys for row in rows)

    us_row = next(row for row in rows if row["code"] == "US")
    assert us_row["name"] == "USA Stocks"
    assert us_row["currency"] == "USD"
    assert us_row["country_iso2"] == "US"


def test_exchange_rows_ignore_unknown_fields() -> None:
    """Unknown exchange payload fields should be ignored."""
    payload = [{"Code": "TEST", "Name": "Test Exchange", "UnknownField": "value"}]
    rows = _exchange_rows(datetime(2026, 1, 2, tzinfo=UTC), payload)
    expected_keys = {"retrieval_date", "code", *EXCHANGE_LIST_COLUMNS}

    assert len(rows) == 1
    assert set(rows[0].keys()) == expected_keys
