from __future__ import annotations

from datetime import date
from functools import reduce
from typing import Iterable

from src.domain.schemas import Assumptions, FinancialModel, LineItems

AVERAGE_WINDOW = 4
DEFAULT_FORECAST_YEARS = 6


def generate_forecast(history: FinancialModel, assumptions: Assumptions) -> FinancialModel:
    historic_items = sorted(history.history, key=lambda item: item.period)
    if not historic_items:
        return FinancialModel(history=[], forecast=[])

    forecast_years = int(assumptions.growth_rates.get("forecast_years", DEFAULT_FORECAST_YEARS))
    if forecast_years <= 0:
        return FinancialModel(history=historic_items, forecast=[])

    ratios = _build_ratios(historic_items, assumptions)
    growth = _build_growth_rates(historic_items, assumptions)
    balance_growth = _build_balance_growth_rates(historic_items, assumptions)

    def step(items: list[LineItems], _: int) -> list[LineItems]:
        next_item = _forecast_next_year(items[-1], ratios, growth, balance_growth)
        return [*items, next_item]

    forecast_items = reduce(step, range(forecast_years), [historic_items[-1]])[1:]
    return FinancialModel(history=historic_items, forecast=forecast_items)


def _build_growth_rates(history: list[LineItems], assumptions: Assumptions) -> dict[str, float]:
    revenue_growth = _average_growth(_series(history, "income", "revenue"))
    shares_growth = _average_growth(_series(history, "income", "shares_diluted"))

    return {
        "revenue": _override(assumptions.growth_rates, "revenue", revenue_growth, 0.0),
        "shares_diluted": _override(assumptions.growth_rates, "shares_diluted", shares_growth, 0.0),
    }


def _build_balance_growth_rates(
    history: list[LineItems],
    assumptions: Assumptions,
) -> dict[str, float]:
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
    return {
        key: _override(
            assumptions.growth_rates,
            key,
            _average_growth(_series(history, "balance", key)) or 0.0,
            0.0,
        )
        for key in balance_keys
    }


def _build_ratios(history: list[LineItems], assumptions: Assumptions) -> dict[str, float]:
    revenue = _series(history, "income", "revenue")

    ratios = {
        "gross_margin": _average_ratio(_series(history, "income", "gross_profit"), revenue),
        "operating_margin": _average_ratio(_series(history, "income", "operating_income"), revenue),
        "tax_rate": _average_ratio(_negate(_series(history, "income", "income_tax")), _series(history, "income", "pre_tax_income")),
        "minorities_rate": _average_ratio(_series(history, "income", "minorities_expense"), _series(history, "income", "net_income")),
        "preferred_dividends_ratio": _average_ratio(
            _series(history, "income", "preferred_dividends"),
            revenue,
        ),
        "payout_ratio": _average_ratio(
            _negate(_series(history, "cash_flow", "dividends_paid")),
            _series(history, "income", "net_income_common"),
        ),
        "depreciation_ratio": _average_ratio(_series(history, "income", "depreciation"), revenue),
        "amortization_ratio": _average_ratio(_series(history, "income", "amortization"), revenue),
        "interest_income_ratio": _average_ratio(_series(history, "income", "interest_income"), revenue),
        "interest_expense_ratio": _average_ratio(_series(history, "income", "interest_expense"), revenue),
        "other_non_operating_ratio": _average_ratio(
            _series(history, "income", "other_non_operating_income"),
            revenue,
        ),
        "affiliates_ratio": _average_ratio(_series(history, "income", "affiliates_income"), revenue),
        "capex_fixed_ratio": _average_ratio(_series(history, "cash_flow", "capex_fixed"), revenue),
        "capex_other_ratio": _average_ratio(_series(history, "cash_flow", "capex_other"), revenue),
        "sale_ppe_ratio": _average_ratio(_series(history, "cash_flow", "sale_ppe"), revenue),
        "working_capital_ratio": _average_ratio(
            _series(history, "cash_flow", "working_capital_change"),
            revenue,
        ),
        "other_cfo_ratio": _average_ratio(_series(history, "cash_flow", "other_cfo"), revenue),
        "other_cfi_ratio": _average_ratio(_series(history, "cash_flow", "other_cfi"), revenue),
        "share_purchases_ratio": _average_ratio(_series(history, "cash_flow", "share_purchases"), revenue),
        "share_sales_ratio": _average_ratio(_series(history, "cash_flow", "share_sales"), revenue),
        "debt_cash_flow_ratio": _average_ratio(_series(history, "cash_flow", "debt_cash_flow"), revenue),
        "other_cff_ratio": _average_ratio(_series(history, "cash_flow", "other_cff"), revenue),
    }

    return {
        key: _override(assumptions.margins, key, value, 0.0)
        for key, value in ratios.items()
    }


def _forecast_next_year(
    prior: LineItems,
    ratios: dict[str, float],
    growth: dict[str, float],
    balance_growth: dict[str, float],
) -> LineItems:
    period = _add_year(prior.period)
    revenue = _apply_growth(prior.income.get("revenue"), growth["revenue"])

    gross_profit = _scale(revenue, ratios["gross_margin"])
    gross_costs = _difference(gross_profit, revenue)
    depreciation = _scale(revenue, ratios["depreciation_ratio"])
    amortization = _scale(revenue, ratios["amortization_ratio"])
    operating_income = _scale(revenue, ratios["operating_margin"])
    other_operating_expenses = _difference(
        operating_income,
        _sum_optional(gross_profit, depreciation, amortization),
    )
    ebitda = _difference(operating_income, _sum_optional(depreciation, amortization))

    interest_income = _scale(revenue, ratios["interest_income_ratio"])
    interest_expense = _scale(revenue, ratios["interest_expense_ratio"])
    other_non_operating = _scale(revenue, ratios["other_non_operating_ratio"])
    pre_tax_income = _sum_optional(
        operating_income,
        interest_income,
        interest_expense,
        other_non_operating,
    )
    income_tax = _scale(pre_tax_income, -ratios["tax_rate"]) if pre_tax_income is not None else None
    affiliates_income = _scale(revenue, ratios["affiliates_ratio"])
    net_income = _sum_optional(pre_tax_income, income_tax, affiliates_income)
    minorities_expense = _scale(net_income, ratios["minorities_rate"])
    preferred_dividends = _scale(revenue, ratios["preferred_dividends_ratio"])
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
    cash_short_term = _cash_from_change(
        prior.balance.get("cash_short_term_investments"),
        cash_flow_items.get("change_in_cash"),
        balance_growth["cash_short_term_investments"],
    )
    balance_items = _forecast_balance_sheet(
        prior.balance,
        balance_growth,
        cash_short_term_override=cash_short_term,
        change_in_cash=cash_flow_items.get("change_in_cash"),
    )
    balance_items["forecast_net_operating_working_capital"] = _operating_working_capital(balance_items)
    balance_items["forecast_net_non_operating_working_capital"] = _non_operating_working_capital(
        balance_items
    )

    return LineItems(period=period, income=income_items, balance=balance_items, cash_flow=cash_flow_items)


def _forecast_balance_sheet(
    prior_balance: dict[str, float | None],
    growth: dict[str, float],
    cash_short_term_override: float | None,
    change_in_cash: float | None,
) -> dict[str, float | None]:
    cash_short_term = (
        cash_short_term_override
        if cash_short_term_override is not None
        else _cash_from_change(
            prior_balance.get("cash_short_term_investments"),
            change_in_cash,
            growth["cash_short_term_investments"],
        )
    )
    inventory = _apply_growth(prior_balance.get("inventory"), growth["inventory"])
    receivables = _apply_growth(prior_balance.get("receivables"), growth["receivables"])
    other_current_assets = _apply_growth(prior_balance.get("other_current_assets"), growth["other_current_assets"])
    current_assets = _sum_optional(cash_short_term, inventory, receivables, other_current_assets)
    if current_assets is None:
        current_assets = _apply_growth(prior_balance.get("current_assets"), growth["current_assets"])

    ppe_net = _apply_growth(prior_balance.get("ppe_net"), growth["ppe_net"])
    software = _apply_growth(prior_balance.get("software"), growth["software"])
    intangibles = _apply_growth(prior_balance.get("intangibles"), growth["intangibles"])
    investments_lt = _apply_growth(prior_balance.get("long_term_investments"), growth["long_term_investments"])
    other_non_current_assets = _apply_growth(
        prior_balance.get("other_non_current_assets"),
        growth["other_non_current_assets"],
    )
    total_non_current_assets = _sum_optional(
        ppe_net,
        software,
        intangibles,
        investments_lt,
        other_non_current_assets,
    )
    if total_non_current_assets is None:
        total_non_current_assets = _apply_growth(
            prior_balance.get("total_non_current_assets"),
            growth["total_non_current_assets"],
        )

    total_assets = _sum_optional(current_assets, total_non_current_assets)
    if total_assets is None:
        total_assets = _apply_growth(prior_balance.get("total_assets"), growth["total_assets"])

    accounts_payable = _apply_growth(prior_balance.get("accounts_payable"), growth["accounts_payable"])
    debt_short_term = _apply_growth(prior_balance.get("debt_short_term"), growth["debt_short_term"])
    debt_long_term = _apply_growth(prior_balance.get("debt_long_term"), growth["debt_long_term"])
    current_liabilities = _apply_growth(prior_balance.get("current_liabilities"), growth["current_liabilities"])
    current_liabilities = _max_optional(
        current_liabilities,
        _sum_optional(accounts_payable, debt_short_term),
    )

    total_liabilities = _apply_growth(prior_balance.get("total_liabilities"), growth["total_liabilities"])
    total_liabilities = _max_optional(
        total_liabilities,
        _sum_optional(current_liabilities, debt_long_term),
    )

    preferred_stock = _apply_growth(prior_balance.get("preferred_stock"), growth["preferred_stock"])
    minority_interest = _apply_growth(prior_balance.get("minority_interest"), growth["minority_interest"])
    total_equity = _difference(total_assets, total_liabilities)
    if total_equity is None:
        total_equity = _apply_growth(prior_balance.get("total_equity"), growth["total_equity"])
    common_equity = _difference(total_equity, _sum_optional(preferred_stock, minority_interest))
    if common_equity is None:
        common_equity = _apply_growth(prior_balance.get("common_equity"), growth["common_equity"])

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
    if operating_working_capital_change is None:
        operating_working_capital_change = _scale(revenue, ratios["working_capital_ratio"])
    if non_operating_working_capital_change is None:
        non_operating_working_capital_change = 0.0
    other_cfo = _scale(revenue, ratios["other_cfo_ratio"])
    if other_cfo is not None:
        other_cfo += non_operating_working_capital_change
    else:
        other_cfo = non_operating_working_capital_change
    cash_from_operations = _sum_optional(
        net_income,
        depreciation,
        amortization,
        operating_working_capital_change,
        other_cfo,
    )

    capex_fixed = _scale(revenue, ratios["capex_fixed_ratio"])
    capex_other = _scale(revenue, ratios["capex_other_ratio"])
    sale_ppe = _scale(revenue, ratios["sale_ppe_ratio"])
    other_cfi = _scale(revenue, ratios["other_cfi_ratio"])
    forecast_total_capex = _sum_optional(capex_fixed, capex_other)
    cash_from_investing = _sum_optional(capex_fixed, capex_other, sale_ppe, other_cfi)

    payout_ratio = ratios["payout_ratio"]
    dividends_paid = _scale(net_income_common, -payout_ratio) if net_income_common is not None else None
    share_purchases = _scale(revenue, ratios["share_purchases_ratio"])
    share_sales = _scale(revenue, ratios["share_sales_ratio"])
    debt_cash_flow = _scale(revenue, ratios["debt_cash_flow_ratio"])
    other_cff = _scale(revenue, ratios["other_cff_ratio"])
    cash_from_financing = _sum_optional(
        dividends_paid,
        share_purchases,
        share_sales,
        debt_cash_flow,
        other_cff,
    )

    change_in_cash = _sum_optional(cash_from_operations, cash_from_investing, cash_from_financing)
    free_cash_flow = _sum_optional(cash_from_operations, capex_fixed, capex_other)
    forecast_dep_amort = _sum_optional(depreciation, amortization)

    return {
        "net_income": net_income,
        "depreciation": depreciation,
        "amortization": amortization,
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
    if section == "income":
        return [item.income.get(key) for item in history]
    if section == "balance":
        return [item.balance.get(key) for item in history]
    if section == "cash_flow":
        return [item.cash_flow.get(key) for item in history]
    return []


def _average_growth(values: list[float | None]) -> float | None:
    rates = [
        current / prior - 1
        for prior, current in zip(values[:-1], values[1:])
        if prior not in (None, 0) and current is not None
    ]
    return _average_tail(rates)


def _average_ratio(numerators: list[float | None], denominators: list[float | None]) -> float | None:
    ratios = [
        numerator / denominator
        for numerator, denominator in zip(numerators, denominators)
        if numerator is not None and denominator not in (None, 0)
    ]
    return _average_tail(ratios)


def _average_tail(values: Iterable[float], window: int = AVERAGE_WINDOW) -> float | None:
    values_list = list(values)
    if not values_list:
        return None
    tail = values_list[-window:]
    return sum(tail) / len(tail)


def _override(source: dict[str, float], key: str, value: float | None, default: float) -> float:
    if key in source:
        return float(source[key])
    if value is None:
        return default
    return float(value)


def _apply_growth(value: float | None, rate: float) -> float | None:
    if value is None:
        return None
    return value * (1 + rate)


def _scale(value: float | None, ratio: float) -> float | None:
    if value is None:
        return None
    return value * ratio


def _sum_optional(*values: float | None) -> float | None:
    if all(value is None for value in values):
        return None
    return sum(value or 0.0 for value in values)


def _difference(value: float | None, other: float | None) -> float | None:
    if value is None or other is None:
        return None
    return value - other


def _max_optional(value: float | None, other: float | None) -> float | None:
    if value is None:
        return other
    if other is None:
        return value
    return max(value, other)


def _negate(values: list[float | None]) -> list[float | None]:
    return [None if value is None else -value for value in values]


def _operating_working_capital(balance: dict[str, float | None]) -> float | None:
    assets = _sum_optional(
        balance.get("inventory"),
        balance.get("receivables"),
    )
    if assets is None:
        return None
    return assets - (balance.get("accounts_payable") or 0.0)


def _other_current_liabilities(balance: dict[str, float | None]) -> float | None:
    current_liabilities = balance.get("current_liabilities")
    if current_liabilities is None:
        return None
    accounts_payable = balance.get("accounts_payable") or 0.0
    debt_short_term = balance.get("debt_short_term") or 0.0
    return current_liabilities - accounts_payable - debt_short_term


def _non_operating_working_capital(balance: dict[str, float | None]) -> float | None:
    other_assets = balance.get("other_current_assets")
    other_liabilities = _other_current_liabilities(balance)
    if other_assets is None or other_liabilities is None:
        return None
    return other_assets - other_liabilities


def _working_capital_change(
    prior_balance: dict[str, float | None],
    current_balance: dict[str, float | None],
) -> float | None:
    prior_wc = _operating_working_capital(prior_balance)
    current_wc = _operating_working_capital(current_balance)
    if prior_wc is None or current_wc is None:
        return None
    return -(current_wc - prior_wc)


def _non_operating_working_capital_change(
    prior_balance: dict[str, float | None],
    current_balance: dict[str, float | None],
) -> float | None:
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
    if prior_cash is not None and change_in_cash is not None:
        return prior_cash + change_in_cash
    return _apply_growth(prior_cash, growth_rate)


def _add_year(period: date) -> date:
    try:
        return period.replace(year=period.year + 1)
    except ValueError:
        return period.replace(year=period.year + 1, day=28)
