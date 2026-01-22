from __future__ import annotations

"""Configuration loader for the application."""

from pathlib import Path
from typing import Any

import tomllib


DEFAULT_REL_TOL = 1e-4
DEFAULT_ABS_TOL = 1e-6

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
    if not config_path.exists():
        _CONFIG_CACHE = {}
        return _CONFIG_CACHE
    _CONFIG_CACHE = tomllib.loads(config_path.read_text(encoding="utf-8"))
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
    rel_tol = database.get("float_rel_tol", DEFAULT_REL_TOL)
    abs_tol = database.get("float_abs_tol", DEFAULT_ABS_TOL)
    try:
        return float(rel_tol), float(abs_tol)
    except (TypeError, ValueError):
        return DEFAULT_REL_TOL, DEFAULT_ABS_TOL
