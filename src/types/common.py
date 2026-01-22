from __future__ import annotations

from datetime import date
from typing import Tuple

from pydantic import BaseModel, ConfigDict, model_validator

Period = date


class TimeSeries(BaseModel):
    model_config = ConfigDict(frozen=True)

    periods: Tuple[Period, ...]
    values: Tuple[float | None, ...]

    @model_validator(mode="after")
    def _validate_lengths(self) -> "TimeSeries":
        """Validate that periods and values are the same length.

        Args:
            self (TimeSeries): The model instance being validated.

        Returns:
            TimeSeries: The validated model instance.
        """
        if len(self.periods) != len(self.values):
            raise ValueError("periods and values must be the same length")
        return self
