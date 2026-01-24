from __future__ import annotations

"""Tests for calendar look-ahead handling in the pipeline."""

import logging
from datetime import timedelta

import main


def test_run_pipeline_caps_calendar_lookahead(monkeypatch, tmp_path, caplog) -> None:
    """Calendar look-ahead should be capped at 30 days."""
    monkeypatch.delenv("HARBOUR_BRIDGE_DB_URL", raising=False)
    monkeypatch.setattr(main, "get_calendar_lookahead_days", lambda: 45)
    monkeypatch.setattr(main, "get_tickers_needing_update", lambda: [])
    monkeypatch.setattr(main, "build_run_data_dir", lambda run_id: tmp_path)
    monkeypatch.setattr(main, "fetch_exchange_list", lambda: [])
    monkeypatch.setattr(main, "fetch_upcoming_earnings", lambda start, end: [])
    monkeypatch.setattr(main, "fetch_upcoming_splits", lambda start, end: [])
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
    dividend_dates = []

    def fake_fetch_dividends(payload_date):
        dividend_dates.append(payload_date)
        return []

    monkeypatch.setattr(main, "fetch_upcoming_dividends", fake_fetch_dividends)

    caplog.set_level(logging.WARNING)
    main.run_pipeline(tmp_path)

    assert len(dividend_dates) == 30
    assert dividend_dates[-1] - dividend_dates[0] == timedelta(days=29)
    assert any("exceeds max 30" in record.message for record in caplog.records)
