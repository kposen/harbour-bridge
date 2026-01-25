from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.engine import Engine

import main


RUN_RETRIEVAL = datetime(2025, 6, 1, tzinfo=UTC)


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


def test_price_refresh_refetches_on_overlap_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Overlap mismatch should trigger a full-history refetch."""
    symbol = "AAPL.US"
    calls: list[date | None] = []

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        calls.append(start_date)
        if start_date is not None:
            payload = [{"date": "2025-01-10", "adjusted_close": "101.0"}]
        else:
            payload = [{"date": "2025-01-10", "adjusted_close": "100.0"}]
        return main.PriceFetchResult(payload=payload, error_code=None, message=None, http_status=None)

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: date(2025, 1, 10))
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (1, 100.0))
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: Path("price.json"))
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 1)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert calls == [date(2025, 1, 10), None]


def test_price_refresh_skips_db_update_when_no_new_rows(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Empty incremental payloads should skip DB updates."""
    symbol = "MSFT.US"
    calls: list[date | None] = []
    writes: list[object] = []

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        calls.append(start_date)
        return main.PriceFetchResult(payload=[], error_code=None, message=None, http_status=None)

    def fake_write_prices(**kwargs: object) -> int:
        writes.append(kwargs)
        return 1

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: date(2025, 2, 1))
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (1, 100.0))
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: Path("price.json"))
    monkeypatch.setattr(main, "write_prices", fake_write_prices)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert calls == [date(2025, 2, 1)]
    assert writes == []


def test_price_refresh_full_history_when_multiple_overlap_rows(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Multiple DB rows on overlap date should force full-history fetch."""
    symbol = "EURUSD.FOREX"
    calls: list[date | None] = []

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        calls.append(start_date)
        payload = [{"date": "2025-03-01", "adjusted_close": "1.1"}]
        return main.PriceFetchResult(payload=payload, error_code=None, message=None, http_status=None)

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: date(2025, 3, 1))
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (2, None))
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: Path("price.json"))
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 1)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert calls == [None]


def test_price_refresh_skips_after_failed_days(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
    symbol_integrity_stub: dict[str, Any],
) -> None:
    """Symbols exceeding the failure threshold should be skipped."""
    symbol = "BAD.SYM"
    calls: list[date | None] = []

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        calls.append(start_date)
        payload = [{"date": "2025-04-01", "adjusted_close": "1.0"}]
        return main.PriceFetchResult(payload=payload, error_code=None, message=None, http_status=None)

    symbol_integrity_stub["failure_days"][(symbol, "prices")] = 7
    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: None)
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (0, None))
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: Path("price.json"))
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 1)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert calls == []
    assert any(row.get("status") == "skipped" for row in symbol_integrity_stub["rows"])


def test_price_refresh_records_failure(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
    symbol_integrity_stub: dict[str, Any],
) -> None:
    """Failures should be recorded in symbol integrity."""
    symbol = "ERR.SYM"

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        return main.PriceFetchResult(payload=None, error_code="http_error", message="404", http_status=404)

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: None)
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (0, None))
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert any(
        row.get("status") == "failed" and row.get("http_status") == 404
        for row in symbol_integrity_stub["rows"]
    )


def test_price_refresh_records_success(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
    symbol_integrity_stub: dict[str, Any],
) -> None:
    """Successful refreshes should be recorded in symbol integrity."""
    symbol = "OK.SYM"

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        payload = [{"date": "2025-05-01", "adjusted_close": "1.0"}]
        return main.PriceFetchResult(payload=payload, error_code=None, message=None, http_status=None)

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: None)
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (0, None))
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: Path("price.json"))
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 1)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert any(row.get("status") == "success" for row in symbol_integrity_stub["rows"])


def test_price_refresh_filters_out_today_payload(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Today's payload should be filtered to avoid intraday overlap refetches."""
    symbol = "TODAY.SYM"
    calls: list[date | None] = []

    def fake_fetch_prices(ticker: str, start_date: date | None) -> main.PriceFetchResult:
        calls.append(start_date)
        payload = [{"date": "2025-06-01", "adjusted_close": "1.0"}]
        return main.PriceFetchResult(payload=payload, error_code=None, message=None, http_status=None)

    monkeypatch.setattr(main, "_fetch_prices_result", fake_fetch_prices)
    monkeypatch.setattr(main, "get_price_refresh_symbols", lambda engine: [symbol])
    monkeypatch.setattr(main, "get_latest_price_date_before", lambda engine, s, c: date(2025, 5, 31))
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, s, d: (1, 1.0))
    monkeypatch.setattr(main, "save_price_payload", lambda *args, **kwargs: Path("price.json"))
    monkeypatch.setattr(main, "write_prices", lambda **kwargs: 1)
    monkeypatch.setattr(main, "get_max_symbols_for_prices", lambda: -1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=RUN_RETRIEVAL,
    )

    assert calls == [date(2025, 5, 31)]
