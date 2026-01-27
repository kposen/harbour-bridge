from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.engine import Engine

import main
from src.io.database import parse_bulk_dividends_csv


def test_parse_bulk_dividends_csv_filters_by_date() -> None:
    """Bulk dividends parser should respect the target_date cutoff."""
    sample_path = Path("data/samples/bulk-dividends-2026-01-25.csv")
    payload = sample_path.read_text(encoding="utf-8")
    rows = parse_bulk_dividends_csv(payload)
    assert any(row["symbol"] == "ADX.US" for row in rows)
    assert all(isinstance(row["date"], date) for row in rows)
    assert parse_bulk_dividends_csv(payload, target_date=date(2026, 1, 25)) == []


def test_bulk_dividends_refresh_schedules_next_cutoff(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
    refresh_schedule_stub: dict[str, Any],
) -> None:
    """Successful bulk dividends refresh should schedule the next cutoff date."""
    sample_csv = "Code,Ex,Date,Dividend,Currency\nADX,US,2026-01-26,0.47,USD\n"
    monkeypatch.setattr(main, "fetch_bulk_dividends", lambda exchange, payload_date: sample_csv)
    monkeypatch.setattr(main, "write_bulk_dividends", lambda **kwargs: 1)
    monkeypatch.setattr(main, "get_exchange_codes", lambda engine: ["US"])
    monkeypatch.setattr(main, "fetch_exchange_share_list", lambda exchange: [])

    run_retrieval = datetime(2026, 1, 27, 9, 0, tzinfo=UTC)
    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=run_retrieval,
    )

    bulk_rows = [
        row for row in refresh_schedule_stub["rows"] if row.get("pipeline") == "bulk_dividends"
    ]
    assert any(row.get("status") == "closed" for row in bulk_rows)
    assert any(
        row.get("status") == "opened" and row.get("refresh_date") == date(2026, 1, 27)
        for row in bulk_rows
    )


def test_bulk_dividends_failure_retries_same_day(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
    refresh_schedule_stub: dict[str, Any],
) -> None:
    """Failure should reschedule retry for the same cutoff date."""
    monkeypatch.setattr(main, "fetch_bulk_dividends", lambda exchange, payload_date: None)
    monkeypatch.setattr(main, "get_exchange_codes", lambda engine: ["US"])
    monkeypatch.setattr(main, "fetch_exchange_share_list", lambda exchange: [])

    run_retrieval = datetime(2026, 1, 27, 12, 0, tzinfo=UTC)
    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=run_retrieval,
    )

    bulk_rows = [
        row for row in refresh_schedule_stub["rows"] if row.get("pipeline") == "bulk_dividends"
    ]
    assert any(
        row.get("status") == "failed" and row.get("refresh_date") == date(2026, 1, 27)
        for row in bulk_rows
    )
