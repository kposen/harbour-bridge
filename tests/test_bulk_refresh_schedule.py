from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest
from sqlalchemy.engine import Engine

import main


def test_bulk_due_refresh_records_cutoff() -> None:
    """Bulk refresh schedule should honor cutoff rules."""
    run_before_cutoff = datetime(2026, 1, 26, 9, 59, tzinfo=UTC)
    run_after_cutoff = datetime(2026, 1, 26, 10, 0, tzinfo=UTC)
    open_records = [
        {"refresh_date": date(2026, 1, 26), "failed_refresh_date": None},
        {"refresh_date": date(2026, 1, 25), "failed_refresh_date": None},
        {"refresh_date": date(2026, 1, 27), "failed_refresh_date": None},
        {"refresh_date": date(2026, 1, 26), "failed_refresh_date": date(2026, 1, 26)},
    ]

    due_before = main._bulk_due_refresh_records(open_records, run_before_cutoff)
    due_after = main._bulk_due_refresh_records(open_records, run_after_cutoff)

    assert open_records[1] in due_before
    assert open_records[3] in due_before
    assert open_records[0] not in due_before
    assert open_records[0] in due_after


def test_next_bulk_cutoff_date() -> None:
    """Next cutoff date should be same day before cutoff, next day after."""
    before = datetime(2026, 1, 26, 9, 59, tzinfo=UTC)
    after = datetime(2026, 1, 26, 10, 0, tzinfo=UTC)

    assert main._next_bulk_cutoff_date(before) == date(2026, 1, 26)
    assert main._next_bulk_cutoff_date(after) == date(2026, 1, 27)


def test_bulk_refresh_full_history_when_stale(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Stale or missing bulk history should trigger full refresh for all symbols."""
    universe_rows = [
        {"symbol": "A.US", "latest_date": None},
        {"symbol": "B.US", "latest_date": date(2025, 1, 1)},
        {"symbol": "C.US", "latest_date": date(2025, 1, 2)},
    ]
    captured: dict[str, Any] = {"symbols": []}

    monkeypatch.setattr(main, "get_filtered_universe_price_status", lambda engine, cutoff: universe_rows)
    monkeypatch.setattr(main, "get_latest_refresh_retrieval", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: 2)

    def fake_run_full_refresh(**kwargs: Any) -> tuple[dict[str, int], set[str]]:
        captured["symbols"] = kwargs.get("symbols")
        summary = {
            "total": len(captured["symbols"]),
            "attempted": len(captured["symbols"]),
            "updated": len(captured["symbols"]),
            "failures": 0,
            "skipped": 0,
            "empty": 0,
        }
        return summary, set(captured["symbols"])

    monkeypatch.setattr(main, "_run_full_price_refreshes", fake_run_full_refresh)

    caplog.set_level(logging.INFO)
    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=datetime(2026, 1, 26, 10, 0, tzinfo=UTC),
    )

    assert captured["symbols"] == ["A.US", "B.US"]
    assert any("Bulk refresh history missing; full refresh required" in rec.message for rec in caplog.records)
