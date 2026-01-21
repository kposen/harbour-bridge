from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from src.domain.schemas import Assumptions
from src.io.reporting import export_model_to_excel
from src.io.storage import save_share_data
from src.logic.forecasting import generate_forecast
from src.logic.historic_builder import build_historic_model


logger = logging.getLogger(__name__)


def get_tickers_needing_update() -> list[str]:
    """Return tickers that should be refreshed by the pipeline.

    Args:
        None

    Returns:
        list[str]: Ticker symbols requiring updates.
    """
    # Allow explicit CLI args; otherwise fall back to the placeholder.
    cli_tickers = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    if cli_tickers:
        return cli_tickers
    # Placeholder: wire this to a watchlist or datastore later.
    return []


def fetch_data(ticker: str) -> dict[str, Any]:
    """Fetch raw provider data for a ticker (network I/O happens here).

    Args:
        ticker (str): The ticker symbol to fetch.

    Returns:
        dict[str, Any]: Raw provider payload for the ticker.
    """
    # Keep side effects in this shell to preserve pure core logic.
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY is not set")
    logger.info("Fetching fundamentals for %s", ticker)
    response = requests.get(
        f"https://eodhd.com/api/fundamentals/{ticker}",
        params={"api_token": api_key, "fmt": "json"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("EODHD response did not return a JSON object")
    logger.debug("Received fundamentals payload keys: %s", sorted(payload.keys()))
    return payload


def run_pipeline(results_dir: Path) -> None:
    """Run the imperative pipeline: fetch -> build history -> forecast -> save.

    Args:
        results_dir (Path): Directory for run outputs.

    Returns:
        None: Side effects are persisted to storage.
    """
    # Keep assumptions in the shell so logic modules stay pure.
    assumptions = Assumptions(growth_rates={}, margins={})
    tickers = get_tickers_needing_update()
    logger.info("Starting pipeline for %d tickers", len(tickers))
    for ticker in tickers:
        logger.info("Processing ticker: %s", ticker)
        # Pull raw data, then build a clean historical model.
        raw_data = fetch_data(ticker)
        historic_model = build_historic_model(raw_data)
        # Add a forecast using placeholder assumptions.
        forecast_model = generate_forecast(historic_model, assumptions)
        # Persist the result as JSON.
        save_share_data(ticker, forecast_model)
        # Write an Excel workbook for each share.
        export_model_to_excel(forecast_model, results_dir / f"{ticker}.xlsx")
    logger.info("Pipeline complete")


def _build_results_dir() -> Path:
    """Create a timestamped results directory for the current run.

    Args:
        None

    Returns:
        Path: Directory path for this run's outputs.
    """
    root = Path(__file__).resolve().parent
    results_root = root / "results"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = results_root / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Created results directory: %s", run_dir)
    return run_dir


if __name__ == "__main__":
    results_dir = _build_results_dir()
    log_path = results_dir / "run.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[console_handler, file_handler])
    run_pipeline(results_dir)
