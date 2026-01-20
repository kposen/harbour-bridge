from __future__ import annotations

from typing import Tuple

from pydantic import BaseModel, ConfigDict


class ShareIdentifier(BaseModel):
    model_config = ConfigDict(frozen=True)

    ticker: str
    exchange: str | None = None
    currency: str | None = None


class ApiRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    identifiers: Tuple[ShareIdentifier, ...]
    fields: Tuple[str, ...]


class ApiResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    identifiers: Tuple[ShareIdentifier, ...]
    fields: Tuple[str, ...]
    data: Tuple[Tuple[float | None, ...], ...]
