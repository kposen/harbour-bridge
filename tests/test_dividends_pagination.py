from __future__ import annotations

from datetime import date
from typing import Any

import main


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._payload


def test_fetch_upcoming_dividends_follows_pagination(
    monkeypatch: Any,
) -> None:
    pages = [
        {
            "data": [{"code": "AAA", "date": "2026-01-01"}],
            "links": {"next": "https://example.com/page2"},
        },
        {
            "data": [{"code": "BBB", "date": "2026-01-01"}],
            "links": {"next": None},
        },
    ]
    calls: list[dict[str, object]] = []

    def fake_get(url: str, params: dict[str, str] | None = None, timeout: int | None = None) -> _FakeResponse:
        calls.append({"url": url, "params": params, "timeout": timeout})
        payload = pages.pop(0)
        return _FakeResponse(payload)

    monkeypatch.setenv("EODHD_API_KEY", "test")
    monkeypatch.setattr(main.requests, "get", fake_get)

    result = main.fetch_upcoming_dividends(date(2026, 1, 27))

    assert isinstance(result, list)
    assert [row.get("code") for row in result if isinstance(row, dict)] == ["AAA", "BBB"]
    assert calls[0]["params"] is not None
    assert calls[1]["params"] is None
