from __future__ import annotations

from datetime import date
from typing import Mapping

from pydantic import BaseModel


class LineItems(BaseModel):
    model_config = {"frozen": True}

    period: date
    income: Mapping[str, float | None]
    balance: Mapping[str, float | None]
    cash_flow: Mapping[str, float | None]


class FinancialModel(BaseModel):
    model_config = {"frozen": True}

    history: list[LineItems]
    forecast: list[LineItems]


class Assumptions(BaseModel):
    model_config = {"frozen": True}

    growth_rates: Mapping[str, float]
    margins: Mapping[str, float]


class ShareMetadata(BaseModel):
    model_config = {"frozen": True}

    ticker: str
    sector: str
    reporting_date: date
