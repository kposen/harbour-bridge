from __future__ import annotations

from datetime import date
from typing import Literal, Tuple

from pydantic import BaseModel, ConfigDict


class ScoringParameter(BaseModel):
    model_config = ConfigDict(frozen=True)

    midpoint: float
    stretch: float
    weight: float
    mode: Literal["log", "linear"]


class ValuationParameters(BaseModel):
    model_config = ConfigDict(frozen=True)

    fcf_yield: ScoringParameter
    roce: ScoringParameter
    growth_rate: ScoringParameter
    min_ln1p_score: float
    max_ln1p_score: float


class ExitAssumptions(BaseModel):
    model_config = ConfigDict(frozen=True)

    metric: str
    multiple: float
    basis: float
    price: float | None = None


class TotalReturnInputs(BaseModel):
    model_config = ConfigDict(frozen=True)

    buy: float
    dividends: Tuple[float, ...]
    sell: float | None = None


class ValuationSeries(BaseModel):
    model_config = ConfigDict(frozen=True)

    periods: Tuple[date, ...]
    price: Tuple[float | None, ...]
    eps: Tuple[float | None, ...]
    fcf_adj_per_share: Tuple[float | None, ...]
    p_tnav: Tuple[float | None, ...]
    pe: Tuple[float | None, ...]
    p_fcf: Tuple[float | None, ...]
    fcf_yield: Tuple[float | None, ...]


class Valuation(BaseModel):
    model_config = ConfigDict(frozen=True)

    parameters: ValuationParameters
    exit_assumptions: ExitAssumptions
    total_return: TotalReturnInputs
    series: ValuationSeries
