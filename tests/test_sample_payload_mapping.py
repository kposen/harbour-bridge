from __future__ import annotations

"""Tests mapping from the sample payload into the data model."""

import json
from datetime import date
from pathlib import Path
from typing import Any

from src.domain.schemas import LineItems

from src.logic.historic_builder import build_historic_model


def _load_payload() -> dict[str, Any]:
    """Load the sample payload JSON from docs.

    Args:
        None

    Returns:
        dict[str, Any]: Parsed JSON payload.
    """
    payload_path = Path(__file__).resolve().parents[1] / "docs" / "sample-payload.txt"
    payload = json.loads(payload_path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("Sample payload is not a JSON object")
    return payload


def _find_item(history: list[LineItems], period: date) -> LineItems:
    """Find a LineItems entry by period.

    Args:
        history (list[LineItems]): Sequence of LineItems objects.
        period (date): The period to match.

    Returns:
        LineItems: The matching LineItems entry.
    """
    return next(item for item in history if item.period == period)


def test_outstanding_shares_are_mapped_to_income() -> None:
    """Outstanding shares should map into income.shares_diluted.

    Args:
        None

    Returns:
        None: Assertions validate mapping behavior.
    """
    payload = _load_payload()
    model = build_historic_model(payload)

    # Verify expected values for specific years.
    item_2025 = _find_item(model.history, date(2025, 9, 30))
    assert item_2025.income["shares_diluted"] == 15004697000.0

    item_2024 = _find_item(model.history, date(2024, 9, 30))
    assert item_2024.income["shares_diluted"] == 15150865000.0
