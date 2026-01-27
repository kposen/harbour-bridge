from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.engine import Engine

import main
from src.io.database import parse_price_history_csv


def test_parse_price_history_csv_skips_overlap() -> None:
    """Price parser should skip rows up to the min_date_exclusive."""
    sample_path = Path("data/samples/MCD.US.prices.csv")
    payload = sample_path.read_text(encoding="utf-8")
    rows = parse_price_history_csv(
        payload=payload,
        symbol="MCD.US",
        provider="EODHD",
        retrieval_date=datetime(2026, 1, 27, tzinfo=UTC),
        min_date_exclusive=date(1966, 7, 5),
    )
    assert rows
    assert all(row["date"] > date(1966, 7, 5) for row in rows)


def test_price_history_partial_uses_latest_date(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Price history refresh should request from the latest stored date."""
    requested: dict[str, Any] = {}

    def fake_fetch(symbol: str, start_date: date | None = None) -> str:
        requested["symbol"] = symbol
        requested["start_date"] = start_date
        return (
            "Date,Open,High,Low,Close,Adjusted_close,Volume\n"
            "2020-01-01,1,1,1,1,1,10\n"
            "2020-01-02,2,2,2,2,2,20\n"
        )

    monkeypatch.setattr(main, "get_filtered_universe_symbols", lambda engine: ["MCD.US"])
    monkeypatch.setattr(main, "get_latest_price_date", lambda engine, symbol: date(2020, 1, 1))
    monkeypatch.setattr(
        main,
        "get_price_day_snapshot",
        lambda engine, symbol, price_date: {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
        },
    )
    monkeypatch.setattr(main, "fetch_price_history", fake_fetch)
    monkeypatch.setattr(main, "write_price_history", lambda *args, **kwargs: 1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=datetime(2026, 1, 27, 12, 0, tzinfo=UTC),
    )

    assert requested["symbol"] == "MCD.US"
    assert requested["start_date"] == date(2020, 1, 1)


def test_price_history_overlap_mismatch_triggers_full_refresh(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Mismatch on overlap OHLC should trigger a full refresh call."""
    calls: list[date | None] = []

    def fake_fetch(symbol: str, start_date: date | None = None) -> str:
        calls.append(start_date)
        if start_date is None:
            return (
                "Date,Open,High,Low,Close,Adjusted_close,Volume\n"
                "2020-01-01,1,1,1,1,1,10\n"
                "2020-01-02,2,2,2,2,2,20\n"
            )
        return (
            "Date,Open,High,Low,Close,Adjusted_close,Volume\n"
            "2020-01-01,9,9,9,9,1,10\n"
            "2020-01-02,2,2,2,2,2,20\n"
        )

    monkeypatch.setattr(main, "get_filtered_universe_symbols", lambda engine: ["MCD.US"])
    monkeypatch.setattr(main, "get_latest_price_date", lambda engine, symbol: date(2020, 1, 1))
    monkeypatch.setattr(
        main,
        "get_price_day_snapshot",
        lambda engine, symbol, price_date: {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
        },
    )
    monkeypatch.setattr(main, "fetch_price_history", fake_fetch)
    monkeypatch.setattr(main, "write_price_history", lambda *args, **kwargs: 1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=datetime(2026, 1, 27, 12, 0, tzinfo=UTC),
    )

    assert calls == [date(2020, 1, 1), None]


def test_price_history_missing_overlap_triggers_full_refresh(
    monkeypatch: pytest.MonkeyPatch,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Missing overlap row should trigger a full refresh call."""
    calls: list[date | None] = []

    def fake_fetch(symbol: str, start_date: date | None = None) -> str:
        calls.append(start_date)
        if start_date is None:
            return (
                "Date,Open,High,Low,Close,Adjusted_close,Volume\n"
                "2020-01-01,1,1,1,1,1,10\n"
                "2020-01-02,2,2,2,2,2,20\n"
            )
        return "Date,Open,High,Low,Close,Adjusted_close,Volume\n2020-01-02,2,2,2,2,2,20\n"

    monkeypatch.setattr(main, "get_filtered_universe_symbols", lambda engine: ["MCD.US"])
    monkeypatch.setattr(main, "get_latest_price_date", lambda engine, symbol: date(2020, 1, 1))
    monkeypatch.setattr(
        main,
        "get_price_day_snapshot",
        lambda engine, symbol, price_date: {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
        },
    )
    monkeypatch.setattr(main, "fetch_price_history", fake_fetch)
    monkeypatch.setattr(main, "write_price_history", lambda *args, **kwargs: 1)

    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
        run_retrieval=datetime(2026, 1, 27, 12, 0, tzinfo=UTC),
    )

    assert calls == [date(2020, 1, 1), None]
