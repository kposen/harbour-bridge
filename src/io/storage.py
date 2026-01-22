from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from src.domain.schemas import FinancialModel


logger = logging.getLogger(__name__)


DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def save_share_data(ticker: str, data: FinancialModel) -> None:
    """Serialize a FinancialModel to JSON on disk under the data directory.

    Args:
        ticker (str): The ticker symbol to persist.
        data (FinancialModel): The model to serialize and store.

    Returns:
        None: Writes the JSON payload to disk.
    """
    # Keep filesystem side effects here so core logic remains pure.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = _share_path(ticker)
    # Use pydantic to produce JSON-friendly data.
    payload = data.model_dump(mode="json")
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    logger.debug("Saved share data to %s", path)


def load_share_data(ticker: str) -> Optional[FinancialModel]:
    """Load a FinancialModel from JSON, returning None when missing.

    Args:
        ticker (str): The ticker symbol to load.

    Returns:
        Optional[FinancialModel]: The parsed model, or None when missing.
    """
    path = _share_path(ticker)
    # No data on disk: treat as a cache miss.
    if not path.exists():
        logger.debug("No share data found for %s", ticker)
        return None
    # Validate payload to keep data consistent.
    payload = json.loads(path.read_text(encoding="utf-8"))
    logger.debug("Loaded share data from %s", path)
    return FinancialModel.model_validate(payload)


def _share_path(ticker: str) -> Path:
    """Build a filesystem path for a ticker's JSON payload.

    Args:
        ticker (str): The ticker symbol to normalize into a filename.

    Returns:
        Path: The filesystem location for the ticker payload.
    """
    # Normalize to avoid duplicate files for different cases.
    normalized = _normalize_ticker(ticker)
    return DATA_DIR / f"{normalized}.json"


def build_run_data_dir(run_id: str) -> Path:
    """Create a timestamped data directory for raw payloads.

    Args:
        run_id (str): Timestamp identifier for the run.

    Returns:
        Path: Directory path for this run's raw payloads.
    """
    run_dir = DATA_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def save_raw_payload(run_dir: Path, ticker: str, payload: dict[str, object]) -> Path:
    """Persist the raw provider payload to the run data directory.

    Args:
        run_dir (Path): Run-specific data directory.
        ticker (str): Ticker symbol for the payload.
        payload (dict[str, object]): Raw provider payload.

    Returns:
        Path: Path to the saved JSON payload.
    """
    normalized = _normalize_ticker(ticker)
    path = run_dir / f"{normalized}.fundamentals.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    logger.debug("Saved raw payload to %s", path)
    return path


def save_price_payload(run_dir: Path, ticker: str, payload: object) -> Path:
    """Persist the raw price payload to the run data directory.

    Args:
        run_dir (Path): Run-specific data directory.
        ticker (str): Ticker symbol for the payload.
        payload (object): Raw provider payload for prices.

    Returns:
        Path: Path to the saved JSON payload.
    """
    normalized = _normalize_ticker(ticker)
    path = run_dir / f"{normalized}.prices.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    logger.debug("Saved price payload to %s", path)
    return path


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker symbols for consistent filenames.

    Args:
        ticker (str): Raw ticker symbol.

    Returns:
        str: Uppercased, trimmed ticker symbol.
    """
    return ticker.strip().upper()
