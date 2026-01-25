from __future__ import annotations

"""Tests for calendar look-ahead handling in the pipeline."""

import logging
from datetime import timedelta
from typing import Any, cast

import pytest
from sqlalchemy.engine import Engine

import main


def test_run_download_pipeline_caps_calendar_lookahead(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Calendar look-ahead should be capped at 30 days."""
    monkeypatch.setattr(main, "get_calendar_lookahead_days", lambda: 45)
    dividend_dates = download_pipeline_stubs["dividend_dates"]

    caplog.set_level(logging.WARNING)
    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
    )

    assert len(dividend_dates) == 30
    assert dividend_dates[-1] - dividend_dates[0] == timedelta(days=29)
    assert any("exceeds max 30" in record.message for record in caplog.records)


def test_run_download_pipeline_floor_calendar_lookahead(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    download_pipeline_stubs: dict[str, Any],
) -> None:
    """Calendar look-ahead should be floored at one day."""
    monkeypatch.setattr(main, "get_calendar_lookahead_days", lambda: 0)
    dividend_dates = download_pipeline_stubs["dividend_dates"]

    caplog.set_level(logging.WARNING)
    main.run_download_pipeline(
        download_pipeline_stubs["tmp_path"],
        [],
        engine=cast(Engine, object()),
    )

    assert len(dividend_dates) == 1
    assert any("invalid; using 1" in record.message for record in caplog.records)
