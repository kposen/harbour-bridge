from __future__ import annotations

from typing import Any

from src.domain.schemas import Assumptions
from src.io.storage import save_share_data
from src.logic.forecasting import generate_forecast
from src.logic.historic_builder import build_historic_model


def get_tickers_needing_update() -> list[str]:
    """Return tickers that should be refreshed by the pipeline.

    Args:
        None

    Returns:
        list[str]: Ticker symbols requiring updates.
    """
    # Placeholder: wire this to a watchlist or datastore later.
    return []


def fetch_data(ticker: str) -> dict[str, Any]:
    """Fetch raw provider data for a ticker (network I/O happens here).

    Args:
        ticker (str): The ticker symbol to fetch.

    Returns:
        dict[str, Any]: Raw provider payload for the ticker.
    """
    # Keep side effects in this shell; raise until a client is available.
    raise NotImplementedError("fetch_data is not wired yet")


def run_pipeline() -> None:
    """Run the imperative pipeline: fetch -> build history -> forecast -> save.

    Args:
        None

    Returns:
        None: Side effects are persisted to storage.
    """
    # Keep assumptions in the shell so logic modules stay pure.
    assumptions = Assumptions(growth_rates={}, margins={})
    for ticker in get_tickers_needing_update():
        # Pull raw data, then build a clean historical model.
        raw_data = fetch_data(ticker)
        historic_model = build_historic_model(raw_data)
        # Add a forecast using placeholder assumptions.
        forecast_model = generate_forecast(historic_model, assumptions)
        # Persist the result as JSON.
        save_share_data(ticker, forecast_model)


if __name__ == "__main__":
    run_pipeline()
