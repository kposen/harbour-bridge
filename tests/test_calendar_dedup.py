from __future__ import annotations

from datetime import UTC, date, datetime

import logging

import pytest

from src.io.database import _dedupe_calendar_rows, _iter_earnings_calendar_rows, write_corporate_actions_calendar


RUN_RETRIEVAL = datetime(2026, 1, 27, 16, 32, tzinfo=UTC)


def test_dedupe_calendar_rows_removes_duplicates() -> None:
    """Duplicate calendar rows should be removed before insertion."""
    rows = [
        {"symbol": "AAA.US", "date": date(2026, 1, 28), "retrieval_date": RUN_RETRIEVAL},
        {"symbol": "AAA.US", "date": date(2026, 1, 28), "retrieval_date": RUN_RETRIEVAL},
        {"symbol": "BBB.US", "date": date(2026, 1, 28), "retrieval_date": RUN_RETRIEVAL},
    ]
    deduped, removed = _dedupe_calendar_rows(rows, ("symbol", "date", "retrieval_date"))

    assert removed == 1
    assert len(deduped) == 2
    assert deduped[0]["symbol"] == "AAA.US"
    assert deduped[1]["symbol"] == "BBB.US"


def test_earnings_calendar_payload_dedupes() -> None:
    """Earnings calendar rows should be deduped after parsing."""
    payload = [
        {"code": "MIVOF.US", "report_date": "2026-01-28"},
        {"code": "MIVOF.US", "report_date": "2026-01-28"},
    ]
    rows = list(_iter_earnings_calendar_rows(RUN_RETRIEVAL, payload))
    deduped, removed = _dedupe_calendar_rows(rows, ("symbol", "date", "retrieval_date"))

    assert len(rows) == 2
    assert len(deduped) == 1
    assert removed == 1


def test_corporate_actions_calendar_logs_dedupes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Deduping should log a warning when duplicates are removed."""
    payload = [
        {"code": "MIVOF.US", "report_date": "2026-01-28"},
        {"code": "MIVOF.US", "report_date": "2026-01-28"},
    ]
    class DummyResult:
        """Minimal execute result stub."""

        def mappings(self) -> "DummyResult":
            return self

        def first(self) -> None:
            return None

    class DummyConn:
        """Minimal connection stub for calendar writes."""

        def execute(self, *args: object, **kwargs: object) -> DummyResult:
            return DummyResult()

    class DummyEngine:
        """Minimal engine stub to avoid hitting the database."""

        def begin(self) -> "DummyEngine":
            return self

        def __enter__(self) -> DummyConn:
            return DummyConn()

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    caplog.set_level(logging.WARNING)
    inserted = write_corporate_actions_calendar(
        engine=DummyEngine(),  # type: ignore[arg-type]
        retrieval_date=RUN_RETRIEVAL,
        earnings_payload=payload,
        splits_payload=[],
        dividends_payloads=[],
    )

    assert inserted == 1
    assert any("Removed 1 duplicate earnings calendar rows" in rec.message for rec in caplog.records)
