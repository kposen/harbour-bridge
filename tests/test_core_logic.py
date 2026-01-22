from __future__ import annotations

"""Tests for core pure logic: history parsing and forecasting."""

from datetime import date
from operator import attrgetter

from more_itertools import first

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
                "totalRevenue": 200.0,
                "grossProfit": 80.0,
                "totalAssets": 100.0,
                "totalCurrentAssets": 40.0,
                "cashAndShortTermInvestments": 10.0,
                "inventory": 5.0,
                "netReceivables": 15.0,
                "totalLiab": 35.0,
                "totalStockholderEquity": 50.0,
                "preferredStock": 10.0,
                "minorityInterest": 5.0,
            }
        ]
    }

    model = build_historic_model(raw_data)

    # Validate a handful of computed and mapped values.
    assert len(model.history) == 1
    item = first(model.history)
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
                    "depreciation": 5.0,
                    "amortization": 3.0,
                    "working_capital_change": 0.0,
                    "other_cfo": 0.0,
                    "cash_from_operations": 49.0,
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
                    "change_in_cash": 38.0,
                    "free_cash_flow": 42.0,
                },
            )
        ],
        forecast=[],
    )

    assumptions = Assumptions(growth_rates={"forecast_years": 2}, margins={})
    forecast_model = generate_forecast(history, assumptions)

    # Each forecast period should satisfy the accounting identity.
    assert len(forecast_model.forecast) == 2
    balances = map(attrgetter("balance"), forecast_model.forecast)
    assert all(
        balance.get("total_assets")
        == balance.get("total_liabilities") + balance.get("total_equity")
        for balance in balances
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


def test_historic_builder_preserves_sign_conventions() -> None:
    """Income statement expenses should be negative; cash flow add-backs positive.

    Args:
        None

    Returns:
        None: Assertions validate sign handling.
    """
    raw_data = {
        "Financials": {
            "Income_Statement": {
                "yearly": {
                    "2025-12-31": {
                        "totalRevenue": "100",
                        "grossProfit": "40",
                        "depreciation": "5",
                        "amortization": "2",
                        "operatingIncome": "20",
                        "incomeBeforeTax": "18",
                        "incomeTax": "4",
                        "netIncome": "14",
                    }
                }
            },
            "Balance_Sheet": {
                "yearly": {
                    "2025-12-31": {
                        "totalAssets": "100",
                        "totalCurrentAssets": "40",
                        "cashAndShortTermInvestments": "10",
                        "inventory": "5",
                        "netReceivables": "15",
                        "totalLiab": "40",
                        "totalStockholderEquity": "60",
                    }
                }
            },
            "Cash_Flow": {
                "yearly": {
                    "2025-12-31": {
                        "netIncome": "14",
                        "depreciation": "5",
                        "amortization": "2",
                        "totalCashFromOperatingActivities": "21",
                    }
                }
            },
        }
    }
    model = build_historic_model(raw_data)
    item = model.history[0]
    # Expenses should be negative on the income statement.
    assert item.income["depreciation"] == -5.0
    assert item.income["amortization"] == -2.0
    # Cash flow add-backs should be positive.
    assert item.cash_flow["depreciation"] == 5.0
    assert item.cash_flow["amortization"] == 2.0
    assert item.cash_flow["net_income"] == 14.0


def test_forecast_working_capital_change_is_negative_on_increase() -> None:
    """Working capital increases should produce negative cash flow impacts.

    Args:
        None

    Returns:
        None: Assertions validate working capital sign.
    """
    history = FinancialModel(
        history=[
            LineItems(
                period=date(2024, 12, 31),
                income={"revenue": 100.0, "net_income": 10.0, "net_income_common": 10.0},
                balance={
                    "inventory": 10.0,
                    "receivables": 20.0,
                    "accounts_payable": 5.0,
                    "cash_short_term_investments": 10.0,
                    "current_assets": 40.0,
                    "total_assets": 100.0,
                    "total_liabilities": 40.0,
                    "total_equity": 60.0,
                },
                cash_flow={"net_income": 10.0},
            )
        ],
        forecast=[],
    )
    # Force growth in inventory/receivables so WC increases.
    assumptions = Assumptions(
        growth_rates={
            "forecast_years": 1,
            "inventory": 0.1,
            "receivables": 0.1,
            "accounts_payable": 0.0,
        },
        margins={},
    )
    forecast_model = generate_forecast(history, assumptions)
    wc_change = forecast_model.forecast[0].cash_flow.get("working_capital_change")
    # Increase in WC should show as a negative cash flow.
    assert wc_change is not None
    assert wc_change < 0
