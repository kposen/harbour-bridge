from __future__ import annotations

"""Validation helpers for provider payloads."""

from typing import Any, Mapping


def validate_eodhd_payload(raw_data: Mapping[str, Any]) -> list[str]:
    """Validate key sections in an EODHD fundamentals payload.

    Args:
        raw_data (Mapping[str, Any]): Raw provider payload.

    Returns:
        list[str]: Human-readable validation warnings.
    """
    warnings: list[str] = []
    if not isinstance(raw_data.get("General"), Mapping):
        warnings.append("Missing General section")
    else:
        general = raw_data["General"]
        if not general.get("Code") and not general.get("PrimaryTicker"):
            warnings.append("General.Code/PrimaryTicker missing")
    financials = raw_data.get("Financials")
    if not isinstance(financials, Mapping):
        warnings.append("Missing Financials section")
    else:
        for key in ("Income_Statement", "Balance_Sheet", "Cash_Flow"):
            statement = financials.get(key)
            if not isinstance(statement, Mapping):
                warnings.append(f"Missing Financials.{key}")
                continue
            if not isinstance(statement.get("yearly"), Mapping):
                warnings.append(f"Missing Financials.{key}.yearly")
            if not isinstance(statement.get("quarterly"), Mapping):
                warnings.append(f"Missing Financials.{key}.quarterly")
    if not isinstance(raw_data.get("outstandingShares"), Mapping):
        warnings.append("Missing outstandingShares section")
    if not isinstance(raw_data.get("Earnings"), Mapping):
        warnings.append("Missing Earnings section")
    if not isinstance(raw_data.get("Holders"), Mapping):
        warnings.append("Missing Holders section")
    if not isinstance(raw_data.get("InsiderTransactions"), Mapping):
        warnings.append("Missing InsiderTransactions section")
    return warnings
