from __future__ import annotations

from datetime import UTC, datetime

from src.io.database import (
    parse_bulk_dividends_csv,
    parse_bulk_prices_csv,
    parse_bulk_splits_csv,
)


RUN_RETRIEVAL = datetime(2026, 1, 26, tzinfo=UTC)


def test_parse_bulk_prices_csv_valid_and_invalid() -> None:
    """Bulk price parsing should return rows and invalid symbols."""
    csv_text = (
        "Code,Ex,Date,Open,High,Low,Close,Adjusted_close,Volume\n"
        "AAA,US,2026-01-25,10,12,9,11,11,1000\n"
        "BAD,US,2026-01-25,10,,9,11,11,1000\n"
    )
    rows, invalid, unknown = parse_bulk_prices_csv(csv_text, RUN_RETRIEVAL, "EODHD")

    assert unknown == 0
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAA.US"
    assert "BAD.US" in invalid


def test_parse_bulk_dividends_csv_valid_and_invalid() -> None:
    """Bulk dividend parsing should handle missing currency."""
    csv_text = (
        "Code,Ex,Date,Dividend,Currency\n"
        "AAA,US,2026-01-25,0.15,USD\n"
        "BAD,US,2026-01-25,0.10,\n"
    )
    rows, invalid, unknown = parse_bulk_dividends_csv(csv_text, RUN_RETRIEVAL)

    assert unknown == 0
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAA.US"
    assert "BAD.US" in invalid


def test_parse_bulk_splits_csv_valid_and_invalid() -> None:
    """Bulk split parsing should handle malformed ratios."""
    csv_text = (
        "Code,Ex,Date,Split\n"
        "AAA,US,2026-01-25,1/2\n"
        "BAD,US,2026-01-25,invalid\n"
    )
    rows, invalid, unknown = parse_bulk_splits_csv(csv_text, RUN_RETRIEVAL)

    assert unknown == 0
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAA.US"
    assert "BAD.US" in invalid
