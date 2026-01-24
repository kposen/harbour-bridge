from __future__ import annotations

"""Tests for calendar look-ahead handling in the pipeline."""

import logging
from datetime import date, timedelta
from pathlib import Path

import pytest

import main


def _stub_download_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    dividend_dates: list[date],
) -> None:
    monkeypatch.setattr(main, "_filter_stale_tickers", lambda tickers, engine: [])
    monkeypatch.setattr(main, "build_run_data_dir", lambda run_id: tmp_path)
    monkeypatch.setattr(main, "fetch_exchange_list", lambda: [])
    monkeypatch.setattr(main, "fetch_upcoming_earnings", lambda start, end: [])
    monkeypatch.setattr(main, "fetch_upcoming_splits", lambda start, end: [])
    monkeypatch.setattr(main, "write_exchange_list", lambda **kwargs: 0)
    monkeypatch.setattr(main, "write_corporate_actions_calendar", lambda **kwargs: 0)
    monkeypatch.setattr(main, "get_exchange_codes", lambda engine: [])
    monkeypatch.setattr(
        main,
        "save_exchanges_list_payload",
        lambda *args, **kwargs: tmp_path / "exchanges-list.json",
    )
    monkeypatch.setattr(
        main,
        "save_upcoming_earnings_payload",
        lambda *args, **kwargs: tmp_path / "upcoming-earnings.json",
    )
    monkeypatch.setattr(
        main,
        "save_upcoming_splits_payload",
        lambda *args, **kwargs: tmp_path / "upcoming-splits.json",
    )
    monkeypatch.setattr(
        main,
        "save_upcoming_dividends_payload",
        lambda *args, **kwargs: tmp_path / "upcoming-dividends.json",
    )

    def fake_fetch_dividends(payload_date: date) -> list[object]:
        dividend_dates.append(payload_date)
        return []

    monkeypatch.setattr(main, "fetch_upcoming_dividends", fake_fetch_dividends)


def test_run_download_pipeline_caps_calendar_lookahead(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Calendar look-ahead should be capped at 30 days."""
    monkeypatch.setattr(main, "get_calendar_lookahead_days", lambda: 45)
    dividend_dates = []
    _stub_download_dependencies(monkeypatch, tmp_path, dividend_dates)

    caplog.set_level(logging.WARNING)
    main.run_download_pipeline(tmp_path, [], engine=object())

    assert len(dividend_dates) == 30
    assert dividend_dates[-1] - dividend_dates[0] == timedelta(days=29)
    assert any("exceeds max 30" in record.message for record in caplog.records)


def test_run_download_pipeline_floor_calendar_lookahead(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Calendar look-ahead should be floored at one day."""
    monkeypatch.setattr(main, "get_calendar_lookahead_days", lambda: 0)
    dividend_dates: list[date] = []
    _stub_download_dependencies(monkeypatch, tmp_path, dividend_dates)

    caplog.set_level(logging.WARNING)
    main.run_download_pipeline(tmp_path, [], engine=object())

    assert len(dividend_dates) == 1
    assert any("invalid; using 1" in record.message for record in caplog.records)
