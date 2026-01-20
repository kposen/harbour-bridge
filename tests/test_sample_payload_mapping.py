from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from src.logic.historic_builder import build_historic_model


def _load_payload() -> dict[str, Any]:
    payload_path = Path(__file__).resolve().parents[1] / "docs" / "sample-payload.txt"
    return json.loads(payload_path.read_text())


def _find_item(history, period: date):
    return next(item for item in history if item.period == period)


def test_outstanding_shares_are_mapped_to_income() -> None:
    payload = _load_payload()
    model = build_historic_model(payload)

    item_2025 = _find_item(model.history, date(2025, 9, 30))
    assert item_2025.income["shares_diluted"] == 15004697000.0

    item_2024 = _find_item(model.history, date(2024, 9, 30))
    assert item_2024.income["shares_diluted"] == 15150865000.0
