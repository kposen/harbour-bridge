from __future__ import annotations

"""Tests for core pure logic: history parsing and forecasting."""

from datetime import date

from src.domain.schemas import Assumptions, FinancialModel, LineItems
from src.logic.forecasting import generate_forecast
from src.logic.historic_builder import build_historic_model


def test_build_historic_model_minimal_payload() -> None:
    """Ensure minimal raw payloads parse into expected LineItems.

    Args:
        None

    Returns:
        None: Assertions validate parsing behavior.
    """
    # Provide enough fields to satisfy accounting identity checks.
    raw_data = {
        "records": [
            {
                "date": "2023-12-31",
                "ff_sales": 200.0,
                "ff_gross_inc": 80.0,
                "ff_assets": 100.0,
                "ff_assets_curr": 40.0,
                "ff_cash_st": 10.0,
                "ff_inven": 5.0,
                "ff_receiv_st": 15.0,
                "ff_liabs": 35.0,
                "ff_com_eq": 50.0,
                "ff_pfd_stk": 10.0,
                "ff_min_int_accum": 5.0,
            }
        ]
    }

    model = build_historic_model(raw_data)

    # Validate a handful of computed and mapped values.
    assert len(model.history) == 1
    item = model.history[0]
    assert item.period == date(2023, 12, 31)
    assert item.income["revenue"] == 200.0
    assert item.income["gross_profit"] == 80.0
    assert item.income["gross_costs"] == -120.0
    assert item.balance["total_assets"] == 100.0
    assert item.balance["total_liabilities"] == 35.0
    assert item.balance["total_equity"] == 65.0


def test_generate_forecast_balance_sheet_identity() -> None:
    """Forecast should produce balanced assets = liabilities + equity.

    Args:
        None

    Returns:
        None: Assertions validate balance sheet identity.
    """
    # Build a small historical model with complete statements.
    history = FinancialModel(
        history=[
            LineItems(
                period=date(2023, 12, 31),
                income={
                    "revenue": 200.0,
                    "gross_profit": 80.0,
                    "gross_costs": -120.0,
                    "depreciation": -5.0,
                    "amortization": -3.0,
                    "other_operating_expenses": -20.0,
                    "operating_income": 52.0,
                    "ebitda": 60.0,
                    "interest_income": 1.0,
                    "interest_expense": -2.0,
                    "other_non_operating_income": 0.0,
                    "pre_tax_income": 51.0,
                    "income_tax": -10.0,
                    "affiliates_income": 0.0,
                    "net_income": 41.0,
                    "minorities_expense": -1.0,
                    "preferred_dividends": -2.0,
                    "net_income_common": 38.0,
                    "shares_diluted": 100.0,
                },
                balance={
                    "cash_short_term_investments": 10.0,
                    "inventory": 5.0,
                    "receivables": 15.0,
                    "other_current_assets": 10.0,
                    "current_assets": 40.0,
                    "ppe_net": 30.0,
                    "software": 5.0,
                    "intangibles": 5.0,
                    "long_term_investments": 10.0,
                    "other_non_current_assets": 10.0,
                    "total_non_current_assets": 60.0,
                    "total_assets": 100.0,
                    "accounts_payable": 5.0,
                    "current_liabilities": 20.0,
                    "total_liabilities": 40.0,
                    "debt_short_term": 5.0,
                    "debt_long_term": 20.0,
                    "preferred_stock": 10.0,
                    "common_equity": 45.0,
                    "minority_interest": 5.0,
                    "total_equity": 60.0,
                },
                cash_flow={
                    "net_income": 41.0,
                    "depreciation": -5.0,
                    "amortization": -3.0,
                    "working_capital_change": 0.0,
                    "other_cfo": 0.0,
                    "cash_from_operations": 33.0,
                    "capex_fixed": -6.0,
                    "capex_other": -1.0,
                    "sale_ppe": 0.0,
                    "other_cfi": 0.0,
                    "cash_from_investing": -7.0,
                    "dividends_paid": -4.0,
                    "share_purchases": 0.0,
                    "share_sales": 0.0,
                    "debt_cash_flow": 0.0,
                    "other_cff": 0.0,
                    "cash_from_financing": -4.0,
                    "change_in_cash": 22.0,
                    "free_cash_flow": 26.0,
                },
            )
        ],
        forecast=[],
    )

    assumptions = Assumptions(growth_rates={"forecast_years": 2}, margins={})
    forecast_model = generate_forecast(history, assumptions)

    # Each forecast period should satisfy the accounting identity.
    assert len(forecast_model.forecast) == 2
    assert all(
        item.balance.get("total_assets")
        == item.balance.get("total_liabilities") + item.balance.get("total_equity")
        for item in forecast_model.forecast
    )


def test_forecast_does_not_mutate_history() -> None:
    """Forecasting should not mutate the original history object.

    Args:
        None

    Returns:
        None: Assertions validate immutability.
    """
    # Keep the history minimal; we only check immutability.
    history = FinancialModel(
        history=[
            LineItems(
                period=date(2023, 12, 31),
                income={"revenue": 100.0},
                balance={"total_assets": 100.0, "total_liabilities": 40.0, "total_equity": 60.0},
                cash_flow={"net_income": 10.0},
            )
        ],
        forecast=[],
    )
    before = history.model_dump()

    # Run forecasting and ensure the original model is unchanged.
    assumptions = Assumptions(growth_rates={"forecast_years": 1}, margins={})
    generate_forecast(history, assumptions)

    assert history.model_dump() == before
