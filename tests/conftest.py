from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import main  # noqa: E402


@pytest.fixture
def refresh_schedule_stub(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub refresh schedule helpers with a mutable state container."""
    state: dict[str, Any] = {
        "next_index": 0,
        "unmatched": {"universe": [], "bulk_dividends": [], "bulk_splits": []},
        "rows": [],
    }

    def fake_append_refresh_schedule_row(**kwargs: object) -> int:
        index = int(state["next_index"])
        state["next_index"] = index + 1
        state["rows"].append({**kwargs, "index": index})
        return index

    monkeypatch.setattr(
        main,
        "get_unmatched_open_refreshes",
        lambda _engine, pipeline: state["unmatched"].get(pipeline, []),
    )
    monkeypatch.setattr(main, "append_refresh_schedule_row", fake_append_refresh_schedule_row)
    return state


@pytest.fixture
def download_pipeline_stubs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    refresh_schedule_stub: dict[str, Any],
) -> dict[str, Any]:
    """Stub common download pipeline dependencies and capture dividend dates."""
    dividend_dates: list[date] = []
    monkeypatch.setattr(main, "_filter_stale_tickers", lambda tickers, engine: [])
    monkeypatch.setattr(main, "get_filtered_universe_symbols", lambda engine: [])
    monkeypatch.setattr(main, "get_latest_price_date", lambda engine, symbol: None)
    monkeypatch.setattr(main, "get_price_day_snapshot", lambda engine, symbol, price_date: None)
    monkeypatch.setattr(main, "build_run_data_dir", lambda run_id: tmp_path)
    monkeypatch.setattr(main, "fetch_exchange_list", lambda: [])
    monkeypatch.setattr(main, "fetch_upcoming_earnings", lambda start, end: [])
    monkeypatch.setattr(main, "fetch_upcoming_splits", lambda start, end: [])
    monkeypatch.setattr(main, "write_exchange_list", lambda **kwargs: 0)
    monkeypatch.setattr(main, "write_corporate_actions_calendar", lambda **kwargs: 0)
    monkeypatch.setattr(main, "fetch_bulk_dividends", lambda exchange, payload_date: "")
    monkeypatch.setattr(main, "write_bulk_dividends", lambda **kwargs: 0)
    monkeypatch.setattr(main, "fetch_bulk_splits", lambda exchange, payload_date: "")
    monkeypatch.setattr(main, "write_bulk_splits", lambda **kwargs: 0)
    monkeypatch.setattr(main, "fetch_price_history", lambda symbol, start_date=None: "")
    monkeypatch.setattr(main, "write_price_history", lambda *args, **kwargs: 0)
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
    monkeypatch.setattr(
        main,
        "save_bulk_dividends_payload",
        lambda *args, **kwargs: tmp_path / "bulk-dividends.csv",
    )
    monkeypatch.setattr(
        main,
        "save_bulk_splits_payload",
        lambda *args, **kwargs: tmp_path / "bulk-splits.csv",
    )
    monkeypatch.setattr(
        main,
        "save_price_history_payload",
        lambda *args, **kwargs: tmp_path / "prices.csv",
    )

    def fake_fetch_dividends(payload_date: date) -> list[object]:
        dividend_dates.append(payload_date)
        return []

    monkeypatch.setattr(main, "fetch_upcoming_dividends", fake_fetch_dividends)
    return {"tmp_path": tmp_path, "dividend_dates": dividend_dates}
