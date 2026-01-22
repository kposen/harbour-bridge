from __future__ import annotations

from datetime import date
from typing import Tuple

from pydantic import BaseModel, ConfigDict, model_validator


class StatementLine(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str | None = None
    label: str
    values: Tuple[float | None, ...]


class Statement(BaseModel):
    model_config = ConfigDict(frozen=True)

    periods: Tuple[date, ...]
    lines: Tuple[StatementLine, ...]

    @model_validator(mode="after")
    def _validate_line_lengths(self) -> "Statement":
        """Validate that all lines match the periods length.

        Args:
            self (Statement): The model instance being validated.

        Returns:
            Statement: The validated model instance.
        """
        expected = len(self.periods)
        for line in self.lines:
            if len(line.values) != expected:
                raise ValueError("all line values must match periods length")
        return self


class IncomeStatement(Statement):
    pass


class BalanceSheet(Statement):
    pass


class CashFlowStatement(Statement):
    pass


class FinancialStatements(BaseModel):
    model_config = ConfigDict(frozen=True)

    income: IncomeStatement
    balance: BalanceSheet
    cash_flow: CashFlowStatement
