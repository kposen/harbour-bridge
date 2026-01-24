from __future__ import annotations

"""Tests for raw payload storage helpers."""

from datetime import date

from src.io.storage import save_exchanges_list_payload, save_upcoming_dividends_payload


def test_save_upcoming_dividends_payload_naming(tmp_path) -> None:
    """Dividends payloads should use the expected filename."""
    payload_date = date(2025, 1, 15)
    path = save_upcoming_dividends_payload(tmp_path, payload_date, [])
    assert path.name == "upcoming-dividends-2025-01-15.json"
    assert path.exists()


def test_save_exchanges_list_payload_naming(tmp_path) -> None:
    """Exchange list payloads should use the expected filename."""
    path = save_exchanges_list_payload(tmp_path, [])
    assert path.name == "exchanges-list.json"
    assert path.exists()
