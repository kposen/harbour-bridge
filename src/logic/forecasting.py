from __future__ import annotations

"""Forecast financial statements using simple averaged assumptions."""

from datetime import date
from functools import partial, reduce
from operator import attrgetter
from typing import Callable, Iterable, Mapping

from more_itertools import pairwise, tail
from toolz import pipe
from toolz.curried import get, map as cmap

from src.domain.schemas import Assumptions, FinancialModel, LineItems

import logging

logger = logging.getLogger(__name__)

AVERAGE_WINDOW = 4
DEFAULT_FORECAST_YEARS = 6

SECTION_GETTERS: dict[str, Callable[[LineItems], Mapping[str, float | None]]] = {
    "income": attrgetter("income"),
    "balance": attrgetter("balance"),
    "cash_flow": attrgetter("cash_flow"),
}


def generate_forecast(history: FinancialModel, assumptions: Assumptions) -> FinancialModel:
    """Generate a forecast from historical data and user assumptions.

    Args:
        history (FinancialModel): Historical model containing past LineItems.
        assumptions (Assumptions): Growth and margin overrides.

    Returns:
        FinancialModel: A model with the same history and populated forecast.
    """
    historic_items = sorted(history.history, key=attrgetter("period"))
    if not historic_items:
        return FinancialModel(history=[], forecast=[])

    # Allow a default horizon, but let assumptions override it.
    forecast_years = int(assumptions.growth_rates.get("forecast_years", DEFAULT_FORECAST_YEARS))
    if forecast_years <= 0:
        return FinancialModel(history=historic_items, forecast=[])

    # Precompute averaged growth rates and ratios from history.
    ratios = _build_ratios(historic_items, assumptions)
    growth = _build_growth_rates(historic_items, assumptions)
    balance_growth = _build_balance_growth_rates(historic_items, assumptions)

    def step(items: list[LineItems], _: int) -> list[LineItems]:
        """Append one forecast period based on the prior period.

        Args:
            items (list[LineItems]): Accumulated history plus forecast items.
            _ (int): Placeholder for the reduction index.

        Returns:
            list[LineItems]: Updated list including the next forecast item.
        """
        # Each forecast period depends on the prior period.
        next_item = _forecast_next_year(items[-1], ratios, growth, balance_growth)
        return [*items, next_item]

    # Use a functional reduction to build successive forecast periods.
    forecast_items = reduce(step, range(forecast_years), [historic_items[-1]])[1:]
    logger.debug("Generated %d forecast periods", len(forecast_items))
    return FinancialModel(history=historic_items, forecast=forecast_items)


def _build_growth_rates(history: list[LineItems], assumptions: Assumptions) -> dict[str, float]:
    """Compute average growth rates with optional overrides.

    Args:
        history (list[LineItems]): Historical LineItems to derive rates from.
        assumptions (Assumptions): Overrides for growth rates.

    Returns:
        dict[str, float]: Growth rates keyed by metric.
    """
    revenue_growth = _average_growth(_series(history, "income", "revenue"))
    shares_growth = _average_growth(_series(history, "income", "shares_diluted"))
    override = partial(_override, assumptions.growth_rates)
    growth_specs = (
        ("revenue", revenue_growth, 0.0),
        ("shares_diluted", shares_growth, 0.0),
    )
    return {key: override(key, value, default) for key, value, default in growth_specs}


def _build_balance_growth_rates(
    history: list[LineItems],
    assumptions: Assumptions,
) -> dict[str, float]:
    """Compute average balance sheet growth rates per line item.

    Args:
        history (list[LineItems]): Historical LineItems to derive rates from.
        assumptions (Assumptions): Overrides for growth rates.

    Returns:
        dict[str, float]: Growth rates keyed by balance sheet item.
    """
    balance_keys = (
        "cash_short_term_investments",
        "inventory",
        "receivables",
        "other_current_assets",
        "current_assets",
        "ppe_net",
        "software",
        "intangibles",
        "long_term_investments",
        "other_non_current_assets",
        "total_non_current_assets",
        "total_assets",
        "accounts_payable",
        "current_liabilities",
        "total_liabilities",
        "debt_short_term",
        "debt_long_term",
        "preferred_stock",
        "common_equity",
        "minority_interest",
        "total_equity",
    )
    override = partial(_override, assumptions.growth_rates)
    series = partial(_series, history, "balance")
    return {
        key: override(key, _average_growth(series(key)) or 0.0, 0.0)
        for key in balance_keys
    }


def _build_ratios(history: list[LineItems], assumptions: Assumptions) -> dict[str, float]:
    """Compute average ratios and margins from historical data.

    Args:
        history (list[LineItems]): Historical LineItems to derive ratios from.
        assumptions (Assumptions): Overrides for margin ratios.

    Returns:
        dict[str, float]: Ratio values keyed by metric.
    """
    revenue = _series(history, "income", "revenue")
    ratio_of = partial(_average_ratio, denominators=revenue)
    ratio_specs = {
        "gross_margin": ratio_of(_series(history, "income", "gross_profit")),
        "operating_margin": ratio_of(_series(history, "income", "operating_income")),
        "tax_rate": _average_ratio(
            _negate(_series(history, "income", "income_tax")),
            _series(history, "income", "pre_tax_income"),
        ),
        "minorities_rate": _average_ratio(
            _series(history, "income", "minorities_expense"),
            _series(history, "income", "net_income"),
        ),
        "preferred_dividends_ratio": ratio_of(_series(history, "income", "preferred_dividends")),
        "payout_ratio": _average_ratio(
            _negate(_series(history, "cash_flow", "dividends_paid")),
            _series(history, "income", "net_income_common"),
        ),
        "depreciation_ratio": ratio_of(_series(history, "income", "depreciation")),
        "amortization_ratio": ratio_of(_series(history, "income", "amortization")),
        "interest_income_ratio": ratio_of(_series(history, "income", "interest_income")),
        "interest_expense_ratio": ratio_of(_series(history, "income", "interest_expense")),
        "other_non_operating_ratio": ratio_of(
            _series(history, "income", "other_non_operating_income"),
        ),
        "affiliates_ratio": ratio_of(_series(history, "income", "affiliates_income")),
        "capex_fixed_ratio": ratio_of(_series(history, "cash_flow", "capex_fixed")),
        "capex_other_ratio": ratio_of(_series(history, "cash_flow", "capex_other")),
        "sale_ppe_ratio": ratio_of(_series(history, "cash_flow", "sale_ppe")),
        "working_capital_ratio": ratio_of(
            _series(history, "cash_flow", "working_capital_change"),
        ),
        "other_cfo_ratio": ratio_of(_series(history, "cash_flow", "other_cfo")),
        "other_cfi_ratio": ratio_of(_series(history, "cash_flow", "other_cfi")),
        "share_purchases_ratio": ratio_of(_series(history, "cash_flow", "share_purchases")),
        "share_sales_ratio": ratio_of(_series(history, "cash_flow", "share_sales")),
        "debt_cash_flow_ratio": ratio_of(_series(history, "cash_flow", "debt_cash_flow")),
        "other_cff_ratio": ratio_of(_series(history, "cash_flow", "other_cff")),
    }
    override = partial(_override, assumptions.margins)
    return {key: override(key, value, 0.0) for key, value in ratio_specs.items()}


def _forecast_next_year(
    prior: LineItems,
    ratios: dict[str, float],
    growth: dict[str, float],
    balance_growth: dict[str, float],
) -> LineItems:
    """Forecast one period ahead from the prior LineItems.

    Args:
        prior (LineItems): The previous period's line items.
        ratios (dict[str, float]): Margin and ratio assumptions.
        growth (dict[str, float]): Growth rates for income items.
        balance_growth (dict[str, float]): Growth rates for balance items.

    Returns:
        LineItems: The forecasted line items for the next period.
    """
    # Advance the period by one year, keeping the same month/day.
    period = _add_year(prior.period)
    logger.debug("Forecasting period %s", period.isoformat())
    # Revenue and shares are grown directly.
    revenue = _apply_growth(prior.income.get("revenue"), growth["revenue"])
    scale_revenue = partial(_scale, revenue)

    # Income statement uses margin-based formulas.
    gross_profit = scale_revenue(ratios["gross_margin"])
    gross_costs = _difference(gross_profit, revenue)
    depreciation = scale_revenue(ratios["depreciation_ratio"])
    amortization = scale_revenue(ratios["amortization_ratio"])
    operating_income = scale_revenue(ratios["operating_margin"])
    other_operating_expenses = _difference(
        operating_income,
        _sum_optional(gross_profit, depreciation, amortization),
    )
    ebitda = _difference(operating_income, _sum_optional(depreciation, amortization))

    # Non-operating items are modeled as ratios to revenue.
    interest_income = scale_revenue(ratios["interest_income_ratio"])
    interest_expense = scale_revenue(ratios["interest_expense_ratio"])
    other_non_operating = scale_revenue(ratios["other_non_operating_ratio"])
    pre_tax_income = _sum_optional(
        operating_income,
        interest_income,
        interest_expense,
        other_non_operating,
    )
    # Taxes are modeled as a rate on pre-tax income (negative expense).
    income_tax = _scale(pre_tax_income, -ratios["tax_rate"]) if pre_tax_income is not None else None
    affiliates_income = scale_revenue(ratios["affiliates_ratio"])
    net_income = _sum_optional(pre_tax_income, income_tax, affiliates_income)
    scale_net_income = partial(_scale, net_income)
    minorities_expense = scale_net_income(ratios["minorities_rate"])
    preferred_dividends = scale_revenue(ratios["preferred_dividends_ratio"])
    net_income_common = _sum_optional(net_income, minorities_expense, preferred_dividends)

    shares_diluted = _apply_growth(prior.income.get("shares_diluted"), growth["shares_diluted"])

    income_items = {
        "revenue": revenue,
        "gross_profit": gross_profit,
        "gross_costs": gross_costs,
        "depreciation": depreciation,
        "amortization": amortization,
        "other_operating_expenses": other_operating_expenses,
        "operating_income": operating_income,
        "ebitda": ebitda,
        "interest_income": interest_income,
        "interest_expense": interest_expense,
        "other_non_operating_income": other_non_operating,
        "pre_tax_income": pre_tax_income,
        "income_tax": income_tax,
        "affiliates_income": affiliates_income,
        "net_income": net_income,
        "minorities_expense": minorities_expense,
        "preferred_dividends": preferred_dividends,
        "net_income_common": net_income_common,
        "shares_diluted": shares_diluted,
    }

    # Preview balance sheet to compute working capital changes.
    preview_balance = _forecast_balance_sheet(
        prior.balance,
        balance_growth,
        cash_short_term_override=None,
        change_in_cash=None,
    )
    operating_wc_change = _working_capital_change(prior.balance, preview_balance)
    non_operating_wc_change = _non_operating_working_capital_change(
        prior.balance,
        preview_balance,
    )
    # Cash flow uses WC deltas from the balance sheet.
    cash_flow_items = _forecast_cash_flow(
        revenue,
        net_income,
        net_income_common,
        depreciation,
        amortization,
        ratios,
        operating_wc_change,
        non_operating_wc_change,
    )
    # Cash is driven by prior cash plus change in cash.
    cash_short_term = _cash_from_change(
        prior.balance.get("cash_short_term_investments"),
        cash_flow_items.get("change_in_cash"),
        balance_growth["cash_short_term_investments"],
    )
    # Final balance sheet aligns cash with the cash flow result.
    balance_items = _forecast_balance_sheet(
        prior.balance,
        balance_growth,
        cash_short_term_override=cash_short_term,
        change_in_cash=cash_flow_items.get("change_in_cash"),
    )
    # Add helper balances used by cash flow.
    balance_items = {
        **balance_items,
        "forecast_net_operating_working_capital": _operating_working_capital(balance_items),
        "forecast_net_non_operating_working_capital": _non_operating_working_capital(
            balance_items
        ),
    }

    return LineItems(period=period, income=income_items, balance=balance_items, cash_flow=cash_flow_items)


def _forecast_balance_sheet(
    prior_balance: dict[str, float | None],
    growth: dict[str, float],
    cash_short_term_override: float | None,
    change_in_cash: float | None,
) -> dict[str, float | None]:
    """Forecast balance sheet values by applying averaged growth rates.

    Args:
        prior_balance (dict[str, float | None]): Prior balance sheet values.
        growth (dict[str, float]): Growth rates for balance sheet items.
        cash_short_term_override (float | None): Cash override from cash flow.
        change_in_cash (float | None): Change in cash from cash flow.

    Returns:
        dict[str, float | None]: Forecast balance sheet values.
    """
    grow = _growth_from(prior_balance, growth)
    cash_short_term = (
        cash_short_term_override
        if cash_short_term_override is not None
        else _cash_from_change(
            prior_balance.get("cash_short_term_investments"),
            change_in_cash,
            growth["cash_short_term_investments"],
        )
    )
    # Current assets built from component line items.
    inventory = grow("inventory")
    receivables = grow("receivables")
    other_current_assets = grow("other_current_assets")
    current_assets = _sum_optional(cash_short_term, inventory, receivables, other_current_assets)
    if current_assets is None:
        current_assets = grow("current_assets")

    ppe_net = grow("ppe_net")
    software = grow("software")
    intangibles = grow("intangibles")
    investments_lt = grow("long_term_investments")
    other_non_current_assets = grow("other_non_current_assets")
    # Non-current assets also roll forward from prior balances.
    total_non_current_assets = _sum_optional(
        ppe_net,
        software,
        intangibles,
        investments_lt,
        other_non_current_assets,
    )
    if total_non_current_assets is None:
        total_non_current_assets = grow("total_non_current_assets")

    # Total assets reconcile from current + non-current where possible.
    total_assets = _sum_optional(current_assets, total_non_current_assets)
    if total_assets is None:
        total_assets = grow("total_assets")

    accounts_payable = grow("accounts_payable")
    debt_short_term = grow("debt_short_term")
    debt_long_term = grow("debt_long_term")
    current_liabilities = grow("current_liabilities")
    current_liabilities = _max_optional(
        current_liabilities,
        _sum_optional(accounts_payable, debt_short_term),
    )

    # Liabilities roll forward and are reconciled from components.
    total_liabilities = grow("total_liabilities")
    total_liabilities = _max_optional(
        total_liabilities,
        _sum_optional(current_liabilities, debt_long_term),
    )

    preferred_stock = grow("preferred_stock")
    minority_interest = grow("minority_interest")
    # Equity is the residual when assets and liabilities are present.
    total_equity = _difference(total_assets, total_liabilities)
    if total_equity is None:
        total_equity = grow("total_equity")
    common_equity = _difference(total_equity, _sum_optional(preferred_stock, minority_interest))
    if common_equity is None:
        common_equity = grow("common_equity")

    return {
        "cash_short_term_investments": cash_short_term,
        "inventory": inventory,
        "receivables": receivables,
        "other_current_assets": other_current_assets,
        "current_assets": current_assets,
        "ppe_net": ppe_net,
        "software": software,
        "intangibles": intangibles,
        "long_term_investments": investments_lt,
        "other_non_current_assets": other_non_current_assets,
        "total_non_current_assets": total_non_current_assets,
        "total_assets": total_assets,
        "accounts_payable": accounts_payable,
        "current_liabilities": current_liabilities,
        "total_liabilities": total_liabilities,
        "debt_short_term": debt_short_term,
        "debt_long_term": debt_long_term,
        "preferred_stock": preferred_stock,
        "common_equity": common_equity,
        "minority_interest": minority_interest,
        "total_equity": total_equity,
    }


def _forecast_cash_flow(
    revenue: float | None,
    net_income: float | None,
    net_income_common: float | None,
    depreciation: float | None,
    amortization: float | None,
    ratios: dict[str, float],
    operating_working_capital_change: float | None,
    non_operating_working_capital_change: float | None,
) -> dict[str, float | None]:
    """Forecast the cash flow statement using income and balance helpers.

    Args:
        revenue (float | None): Forecast revenue.
        net_income (float | None): Forecast net income.
        net_income_common (float | None): Net income attributable to common.
        depreciation (float | None): Forecast depreciation.
        amortization (float | None): Forecast amortization.
        ratios (dict[str, float]): Ratio assumptions.
        operating_working_capital_change (float | None): Operating WC delta.
        non_operating_working_capital_change (float | None): Non-op WC delta.

    Returns:
        dict[str, float | None]: Forecast cash flow line items.
    """
    scale_revenue = partial(_scale, revenue)
    if operating_working_capital_change is None:
        operating_working_capital_change = scale_revenue(ratios["working_capital_ratio"])
    if non_operating_working_capital_change is None:
        non_operating_working_capital_change = 0.0
    # Cash-flow D&A should be positive add-backs.
    negate = partial(_scale, ratio=-1.0)
    depreciation_cfs = negate(depreciation)
    amortization_cfs = negate(amortization)
    # Other CFO is treated as a revenue-based proxy.
    other_cfo = scale_revenue(ratios["other_cfo_ratio"])
    cash_from_operations = _sum_optional(
        net_income,
        depreciation_cfs,
        amortization_cfs,
        operating_working_capital_change,
        non_operating_working_capital_change,
        other_cfo,
    )

    capex_fixed = scale_revenue(ratios["capex_fixed_ratio"])
    capex_other = scale_revenue(ratios["capex_other_ratio"])
    sale_ppe = scale_revenue(ratios["sale_ppe_ratio"])
    other_cfi = scale_revenue(ratios["other_cfi_ratio"])
    forecast_total_capex = _sum_optional(capex_fixed, capex_other)
    cash_from_investing = _sum_optional(capex_fixed, capex_other, sale_ppe, other_cfi)

    # Financing uses payout ratios and revenue-based proxies.
    payout_ratio = ratios["payout_ratio"]
    dividends_paid = _scale(net_income_common, -payout_ratio) if net_income_common is not None else None
    share_purchases = scale_revenue(ratios["share_purchases_ratio"])
    share_sales = scale_revenue(ratios["share_sales_ratio"])
    debt_cash_flow = scale_revenue(ratios["debt_cash_flow_ratio"])
    other_cff = scale_revenue(ratios["other_cff_ratio"])
    cash_from_financing = _sum_optional(
        dividends_paid,
        share_purchases,
        share_sales,
        debt_cash_flow,
        other_cff,
    )

    # Change in cash reconciles three statement sections.
    change_in_cash = _sum_optional(cash_from_operations, cash_from_investing, cash_from_financing)
    free_cash_flow = _sum_optional(cash_from_operations, capex_fixed, capex_other)
    forecast_dep_amort = _sum_optional(depreciation_cfs, amortization_cfs)

    return {
        "net_income": net_income,
        "depreciation": depreciation_cfs,
        "amortization": amortization_cfs,
        "working_capital_change": operating_working_capital_change,
        "forecast_changes_non_operating_working_capital": non_operating_working_capital_change,
        "other_cfo": other_cfo,
        "cash_from_operations": cash_from_operations,
        "capex_fixed": capex_fixed,
        "capex_other": capex_other,
        "forecast_total_capex": forecast_total_capex,
        "sale_ppe": sale_ppe,
        "other_cfi": other_cfi,
        "cash_from_investing": cash_from_investing,
        "dividends_paid": dividends_paid,
        "share_purchases": share_purchases,
        "share_sales": share_sales,
        "debt_cash_flow": debt_cash_flow,
        "other_cff": other_cff,
        "cash_from_financing": cash_from_financing,
        "change_in_cash": change_in_cash,
        "free_cash_flow": free_cash_flow,
        "forecast_depreciation_amortization": forecast_dep_amort,
    }


def _series(history: list[LineItems], section: str, key: str) -> list[float | None]:
    """Extract a series of values from history for one statement section.

    Args:
        history (list[LineItems]): Historical line items.
        section (str): Which statement to read ("income", "balance", "cash_flow").
        key (str): The line item key to extract.

    Returns:
        list[float | None]: Sequence of values for the key.
    """
    getter = SECTION_GETTERS.get(section)
    if getter is None:
        return []
    return list(pipe(history, cmap(getter), cmap(lambda values: values.get(key))))


def _average_growth(values: list[float | None]) -> float | None:
    """Compute average growth over the trailing window.

    Args:
        values (list[float | None]): Time-ordered values.

    Returns:
        float | None: Average growth rate, if available.
    """
    rates = [
        current / prior - 1
        for prior, current in pairwise(values)
        if prior not in (None, 0) and current is not None
    ]
    return _average_tail(rates)


def _average_ratio(numerators: list[float | None], denominators: list[float | None]) -> float | None:
    """Compute average ratio over the trailing window.

    Args:
        numerators (list[float | None]): Numerator series.
        denominators (list[float | None]): Denominator series.

    Returns:
        float | None: Average ratio, if available.
    """
    ratios = [
        numerator / denominator
        for numerator, denominator in zip(numerators, denominators)
        if numerator is not None and denominator not in (None, 0)
    ]
    return _average_tail(ratios)


def _average_tail(values: Iterable[float], window: int = AVERAGE_WINDOW) -> float | None:
    """Average the trailing window of values, if any.

    Args:
        values (Iterable[float]): Values to average.
        window (int): Trailing window length.

    Returns:
        float | None: Average of the trailing window, if any.
    """
    tail_values = list(tail(window, values))
    if not tail_values:
        return None
    return sum(tail_values) / len(tail_values)


def _override(source: dict[str, float], key: str, value: float | None, default: float) -> float:
    """Use an explicit override when provided, otherwise fall back.

    Args:
        source (dict[str, float]): Override dictionary.
        key (str): Key to check in overrides.
        value (float | None): Computed value to use if no override.
        default (float): Fallback when value is missing.

    Returns:
        float: The override or computed value.
    """
    if key in source:
        return float(source[key])
    if value is None:
        return default
    return float(value)


def _apply_growth(value: float | None, rate: float) -> float | None:
    """Apply a growth rate to a value.

    Args:
        value (float | None): Base value.
        rate (float): Growth rate.

    Returns:
        float | None: Grown value, if base exists.
    """
    if value is None:
        return None
    return value * (1 + rate)


def _scale(value: float | None, ratio: float) -> float | None:
    """Scale a value by a ratio.

    Args:
        value (float | None): Base value.
        ratio (float): Ratio multiplier.

    Returns:
        float | None: Scaled value, if base exists.
    """
    if value is None:
        return None
    return value * ratio


def _sum_optional(*values: float | None) -> float | None:
    """Sum optional values, returning None if all are missing.

    Args:
        *values (float | None): Optional numeric values.

    Returns:
        float | None: Sum of values, or None when all missing.
    """
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present)


def _difference(value: float | None, other: float | None) -> float | None:
    """Return value minus other, preserving None when missing.

    Args:
        value (float | None): Minuend value.
        other (float | None): Subtrahend value.

    Returns:
        float | None: Difference, or None when missing.
    """
    if value is None or other is None:
        return None
    return value - other


def _max_optional(value: float | None, other: float | None) -> float | None:
    """Return the maximum when both values exist, otherwise the one present.

    Args:
        value (float | None): First value.
        other (float | None): Second value.

    Returns:
        float | None: Maximum or existing value.
    """
    if value is None:
        return other
    if other is None:
        return value
    return max(value, other)


def _negate(values: list[float | None]) -> list[float | None]:
    """Negate a list of values, preserving None entries.

    Args:
        values (list[float | None]): Values to negate.

    Returns:
        list[float | None]: Negated values.
    """
    return [None if value is None else -value for value in values]


def _growth_from(
    prior_balance: dict[str, float | None],
    growth: dict[str, float],
) -> Callable[[str], float | None]:
    """Curry a balance sheet and growth map into a growth lookup function.

    Args:
        prior_balance (dict[str, float | None]): Prior balance sheet values.
        growth (dict[str, float]): Growth rates keyed by line item.

    Returns:
        Callable[[str], float | None]: Function that grows a specific key.
    """
    def grow(key: str) -> float | None:
        """Apply growth to a specific balance sheet key.

        Args:
            key (str): Balance sheet line item key.

        Returns:
            float | None: Grown value for the key, if present.
        """
        return _apply_growth(prior_balance.get(key), growth[key])

    return grow


def _operating_working_capital(balance: dict[str, float | None]) -> float | None:
    """Compute operating working capital from balance sheet lines.

    Args:
        balance (dict[str, float | None]): Balance sheet values.

    Returns:
        float | None: Operating working capital value.
    """
    assets = _sum_optional(
        balance.get("inventory"),
        balance.get("receivables"),
    )
    if assets is None:
        return None
    return assets - (balance.get("accounts_payable") or 0.0)


def _other_current_liabilities(balance: dict[str, float | None]) -> float | None:
    """Compute current liabilities excluding AP and short-term debt.

    Args:
        balance (dict[str, float | None]): Balance sheet values.

    Returns:
        float | None: Other current liabilities value.
    """
    current_liabilities = balance.get("current_liabilities")
    if current_liabilities is None:
        return None
    accounts_payable = balance.get("accounts_payable") or 0.0
    debt_short_term = balance.get("debt_short_term") or 0.0
    return current_liabilities - accounts_payable - debt_short_term


def _non_operating_working_capital(balance: dict[str, float | None]) -> float | None:
    """Compute non-operating working capital from other current items.

    Args:
        balance (dict[str, float | None]): Balance sheet values.

    Returns:
        float | None: Non-operating working capital value.
    """
    other_assets = balance.get("other_current_assets")
    other_liabilities = _other_current_liabilities(balance)
    if other_assets is None or other_liabilities is None:
        return None
    return other_assets - other_liabilities


def _working_capital_change(
    prior_balance: dict[str, float | None],
    current_balance: dict[str, float | None],
) -> float | None:
    """Compute operating WC change (negative delta).

    Args:
        prior_balance (dict[str, float | None]): Prior balance sheet values.
        current_balance (dict[str, float | None]): Current balance sheet values.

    Returns:
        float | None: Operating working capital change.
    """
    prior_wc = _operating_working_capital(prior_balance)
    current_wc = _operating_working_capital(current_balance)
    if prior_wc is None or current_wc is None:
        return None
    return -(current_wc - prior_wc)


def _non_operating_working_capital_change(
    prior_balance: dict[str, float | None],
    current_balance: dict[str, float | None],
) -> float | None:
    """Compute non-operating WC change (negative delta).

    Args:
        prior_balance (dict[str, float | None]): Prior balance sheet values.
        current_balance (dict[str, float | None]): Current balance sheet values.

    Returns:
        float | None: Non-operating working capital change.
    """
    prior_wc = _non_operating_working_capital(prior_balance)
    current_wc = _non_operating_working_capital(current_balance)
    if prior_wc is None or current_wc is None:
        return None
    return -(current_wc - prior_wc)


def _cash_from_change(
    prior_cash: float | None,
    change_in_cash: float | None,
    growth_rate: float,
) -> float | None:
    """Update cash by change in cash, or fallback to growth.

    Args:
        prior_cash (float | None): Prior cash balance.
        change_in_cash (float | None): Change in cash from cash flow.
        growth_rate (float): Growth rate fallback.

    Returns:
        float | None: Updated cash balance.
    """
    if prior_cash is not None and change_in_cash is not None:
        return prior_cash + change_in_cash
    return _apply_growth(prior_cash, growth_rate)


def _add_year(period: date) -> date:
    """Advance a date by one year, handling leap days safely.

    Args:
        period (date): Date to advance.

    Returns:
        date: Same month/day in the next year (Feb 29 handled).
    """
    try:
        return period.replace(year=period.year + 1)
    except ValueError:
        return period.replace(year=period.year + 1, day=28)
