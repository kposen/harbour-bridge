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
    general = raw_data.get("General")
    financials = raw_data.get("Financials")
    warnings = [
        *([] if isinstance(general, Mapping) else ["Missing General section"]),
        *([] if isinstance(financials, Mapping) else ["Missing Financials section"]),
        *([] if isinstance(raw_data.get("outstandingShares"), Mapping) else ["Missing outstandingShares section"]),
        *([] if isinstance(raw_data.get("Earnings"), Mapping) else ["Missing Earnings section"]),
        *([] if isinstance(raw_data.get("Holders"), Mapping) else ["Missing Holders section"]),
        *([] if isinstance(raw_data.get("InsiderTransactions"), Mapping) else ["Missing InsiderTransactions section"]),
    ]
    if isinstance(general, Mapping) and not (general.get("Code") or general.get("PrimaryTicker")):
        warnings.append("General.Code/PrimaryTicker missing")
    if isinstance(financials, Mapping):
        warnings.extend(
            warning
            for key in ("Income_Statement", "Balance_Sheet", "Cash_Flow")
            for warning in _statement_warnings(financials, key)
        )
    return warnings


def _statement_warnings(financials: Mapping[str, Any], key: str) -> list[str]:
    """Collect warnings for a statement block.

    Args:
        financials (Mapping[str, Any]): Financials payload block.
        key (str): Statement key to inspect.

    Returns:
        list[str]: Warning messages for the statement.
    """
    statement = financials.get(key)
    if not isinstance(statement, Mapping):
        return [f"Missing Financials.{key}"]
    warnings = [
        warning
        for label, field in (("yearly", "yearly"), ("quarterly", "quarterly"))
        if not isinstance(statement.get(field), Mapping)
        for warning in [f"Missing Financials.{key}.{label}"]
    ]
    return warnings
