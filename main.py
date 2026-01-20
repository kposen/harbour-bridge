from __future__ import annotations

from typing import Any

from src.domain.schemas import Assumptions
from src.io.storage import save_share_data
from src.logic.forecasting import generate_forecast
from src.logic.historic_builder import build_historic_model


def get_tickers_needing_update() -> list[str]:
    return []


def fetch_data(ticker: str) -> dict[str, Any]:
    raise NotImplementedError("fetch_data is not wired yet")


def run_pipeline() -> None:
    assumptions = Assumptions(growth_rates={}, margins={})
    for ticker in get_tickers_needing_update():
        raw_data = fetch_data(ticker)
        historic_model = build_historic_model(raw_data)
        forecast_model = generate_forecast(historic_model, assumptions)
        save_share_data(ticker, forecast_model)


if __name__ == "__main__":
    run_pipeline()
