from __future__ import annotations

"""Configuration loader for the application."""

from pathlib import Path
from typing import Any

import tomllib


DEFAULT_REL_TOL = 1e-4
DEFAULT_ABS_TOL = 1e-6
DEFAULT_CALENDAR_LOOKAHEAD_DAYS = 30

_CONFIG_CACHE: dict[str, Any] | None = None


def load_config() -> dict[str, Any]:
    """Load configuration from the repository root config file.

    Args:
        None

    Returns:
        dict[str, Any]: Parsed configuration values.
    """
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE
    config_path = Path(__file__).resolve().parents[1] / "config.toml"
    _CONFIG_CACHE = (
        tomllib.loads(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    )
    return _CONFIG_CACHE


def get_database_tolerances() -> tuple[float, float]:
    """Return float comparison tolerances for database deduplication.

    Args:
        None

    Returns:
        tuple[float, float]: Relative and absolute tolerances.
    """
    config = load_config()
    database = config.get("database", {}) if isinstance(config, dict) else {}
    rel_tol = _coerce_float(database.get("float_rel_tol"), DEFAULT_REL_TOL)
    abs_tol = _coerce_float(database.get("float_abs_tol"), DEFAULT_ABS_TOL)
    return rel_tol, abs_tol


def get_calendar_lookahead_days() -> int:
    """Return the look-ahead window for corporate actions calendars.

    Args:
        None

    Returns:
        int: Look-ahead window in days.
    """
    config = load_config()
    calendar = config.get("calendar", {}) if isinstance(config, dict) else {}
    return _coerce_int(calendar.get("lookahead_days"), DEFAULT_CALENDAR_LOOKAHEAD_DAYS)


def _coerce_float(value: object, default: float) -> float:
    """Coerce a value to float with a default fallback.

    Args:
        value (object): Raw value to convert.
        default (float): Default to return on error.

    Returns:
        float: Parsed float or default.
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _coerce_int(value: object, default: int) -> int:
    """Coerce a value to int with a default fallback."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default
