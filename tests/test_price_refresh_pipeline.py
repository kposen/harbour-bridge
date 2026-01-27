from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.engine import Engine

import main


RUN_RETRIEVAL = datetime(2026, 1, 26, 10, 0, tzinfo=UTC)


def test_apply_price_refresh_limit_prioritizes_oldest(monkeypatch: pytest.MonkeyPatch) -> None:
    """Oldest symbols should be selected first with random tie-breaks."""
    latest = {
        "AAA": None,
        "BBB": date(2024, 1, 1),
        "CCC": date(2024, 1, 1),
        "DDD": date(2025, 1, 1),
    }
    monkeypatch.setattr(main.random, "sample", lambda items, k: items[:k])

    selected = main._apply_price_refresh_limit(latest, 2)

    assert selected == ["AAA", "BBB"]
    assert set(main._apply_price_refresh_limit(latest, -1)) == set(latest.keys())
    assert main._apply_price_refresh_limit(latest, 0) == []


def test_bulk_target_date_cutoff() -> None:
    """Bulk target date should honor the 10:00 UTC cutoff."""
    after_cutoff = datetime(2026, 1, 23, 10, 0, tzinfo=UTC)
    before_cutoff = datetime(2026, 1, 23, 9, 59, tzinfo=UTC)

    assert main._bulk_target_date(after_cutoff) == date(2026, 1, 22)
    assert main._bulk_target_date(before_cutoff) == date(2026, 1, 21)


def test_filter_price_payload_for_cutoff_inclusive() -> None:
    """Cutoff filter should keep rows on the cutoff date."""
    payload = [
        {"date": "2026-01-24", "adjusted_close": "1.0"},
        {"date": "2026-01-25", "adjusted_close": "2.0"},
        {"date": "2026-01-26", "adjusted_close": "3.0"},
    ]
    filtered = main._filter_price_payload_for_cutoff(payload, date(2026, 1, 25))

    assert isinstance(filtered, list)
    assert [entry["date"] for entry in filtered] == ["2026-01-24", "2026-01-25"]


def test_select_full_refresh_symbols_includes_triggers_and_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full refresh selection should include stale/missing and trigger symbols."""
    universe_rows = [
        {"symbol": "OLD.US", "latest_date": date(2025, 1, 1)},
        {"symbol": "NEW.US", "latest_date": date(2025, 5, 9)},
        {"symbol": "NONE.US", "latest_date": None},
    ]

    monkeypatch.setattr(main, "get_filtered_universe_price_status", lambda engine, cutoff: universe_rows)
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, symbol, cutoff: date(2025, 4, 1))

    symbols = main._select_full_refresh_symbols(
        engine=cast(Engine, object()),
        cutoff_date=date(2025, 5, 10),
        stale_days=7,
        max_symbols=-1,
        trigger_symbols={"TRIG.US"},
    )

    assert set(symbols) == {"OLD.US", "NONE.US", "TRIG.US"}


def test_full_price_refresh_records_failure_on_empty_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    symbol_integrity_stub: dict[str, Any],
) -> None:
    """Empty price payloads should record failures for symbol integrity."""
    symbol = "EMPTY.US"

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        return main.PriceFetchResult(
            payload=[],
            raw_text="Date,Open,High,Low,Close,Adjusted_close,Volume\n",
            error_code=None,
            message=None,
            http_status=None,
        )

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: tmp_path / "price.json")
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 0)

    summary, _ = main._run_full_price_refreshes(
        engine=cast(Engine, object()),
        data_dir=tmp_path,
        run_retrieval=RUN_RETRIEVAL,
        symbols=[symbol],
        price_cutoff=date(2026, 1, 25),
        provider="EODHD",
        integrity_skips=[],
    )

    assert summary["failures"] == 1
    assert summary["empty"] == 1
    assert any(
        row.get("status") == "failed" and row.get("error_code") == "empty_payload"
        for row in symbol_integrity_stub["rows"]
    )


def test_full_price_refresh_skips_after_failed_days(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    symbol_integrity_stub: dict[str, Any],
) -> None:
    """Symbols exceeding the failure threshold should be skipped."""
    symbol = "BAD.US"
    symbol_integrity_stub["failure_days"][(symbol, "prices")] = 7

    monkeypatch.setattr(
        main,
        "_fetch_prices_result",
        lambda *args, **kwargs: main.PriceFetchResult(
            payload=[],
            raw_text="Date,Open,High,Low,Close,Adjusted_close,Volume\n",
            error_code=None,
            message=None,
            http_status=None,
        ),
    )
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: tmp_path / "price.json")
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 0)

    integrity_skips: list[str] = []
    summary, _ = main._run_full_price_refreshes(
        engine=cast(Engine, object()),
        data_dir=tmp_path,
        run_retrieval=RUN_RETRIEVAL,
        symbols=[symbol],
        price_cutoff=date(2026, 1, 25),
        provider="EODHD",
        integrity_skips=integrity_skips,
    )

    assert summary["skipped"] == 1
    assert symbol in integrity_skips
    assert any(row.get("status") == "skipped" for row in symbol_integrity_stub["rows"])
