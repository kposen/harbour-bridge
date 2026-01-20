from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from .common import TimeSeries


class PriceSeries(BaseModel):
    model_config = ConfigDict(frozen=True)

    prices: TimeSeries


class ReturnsSeries(BaseModel):
    model_config = ConfigDict(frozen=True)

    returns: TimeSeries
