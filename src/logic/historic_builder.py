from __future__ import annotations

from datetime import date, datetime
from math import isclose
from typing import Any, Mapping

from src.domain.schemas import FinancialModel, LineItems

EODHD_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "revenue": ("totalRevenue",),
    "gross_profit": ("grossProfit",),
    "gross_costs": ("costOfRevenue", "costOfGoodsAndServicesSold"),
    "depreciation": ("depreciation",),
    "amortization": ("amortization",),
    "depreciation_and_amortization": ("depreciationAndAmortization",),
    "operating_income": ("operatingIncome",),
    "interest_income": ("interestIncome",),
    "interest_expense": ("interestExpense",),
    "pre_tax_income": ("incomeBeforeTax",),
    "income_tax": ("incomeTax", "incomeTaxExpense"),
    "affiliates_income": ("equityEarnings",),
    "net_income": ("netIncome",),
    "minorities_expense": ("minorityInterest",),
    "preferred_dividends": ("preferredDividends",),
    "shares_diluted": ("dilutedSharesOutstanding", "weightedAverageShsOutDil"),
    "cash_short_term_investments": ("cashAndShortTermInvestments",),
    "inventory": ("inventory",),
    "receivables": ("netReceivables",),
    "current_assets": ("totalCurrentAssets",),
    "ppe_net": ("propertyPlantEquipmentNet",),
    "software": ("software",),
    "intangibles": ("intangibleAssets",),
    "long_term_investments": ("longTermInvestments",),
    "total_assets": ("totalAssets",),
    "current_liabilities": ("totalCurrentLiabilities",),
    "accounts_payable": ("accountsPayable",),
    "total_liabilities": ("totalLiab",),
    "debt_short_term": ("shortTermDebt",),
    "debt_long_term": ("longTermDebt",),
    "preferred_stock": ("preferredStock",),
    "common_equity": ("totalStockholderEquity", "commonStockEquity"),
    "minority_interest": ("minorityInterest",),
    "net_income_cfs": ("netIncome",),
    "working_capital_change": ("changeInWorkingCapital",),
    "cash_from_operations": ("totalCashFromOperatingActivities",),
    "capex_fixed": ("capitalExpenditures",),
    "capex_other": ("otherCapitalExpenditures",),
    "sale_ppe": ("saleOfPPE",),
    "cash_from_investing": ("totalCashflowsFromInvestingActivities",),
    "dividends_paid": ("dividendsPaid",),
    "share_purchases": ("stockRepurchase",),
    "share_sales": ("issuanceOfStock",),
    "debt_cash_flow": ("cashFromDebt",),
    "cash_from_financing": ("totalCashFromFinancingActivities",),
}

FACTSET_FIELD_MAP: dict[str, tuple[str, ...]] = {}


def build_historic_model(
    raw_data: dict[str, Any],
    field_map: Mapping[str, tuple[str, ...]] = EODHD_FIELD_MAP,
) -> FinancialModel:
    records = _extract_records(raw_data)
    for record in records:
        if record.get("date") is None:
            raise ValueError("record date is missing or invalid")
    history = [
        _build_line_items(record, field_map)
        for record in sorted(records, key=lambda item: item["date"])
    ]
    return FinancialModel(history=history, forecast=[])


def _extract_records(raw_data: Mapping[str, Any]) -> list[dict[str, Any]]:
    shares_by_date = _extract_outstanding_shares(raw_data)
    if "records" in raw_data and isinstance(raw_data["records"], list):
        records = [
            {**dict(item), "date": _parse_date(dict(item).get("date"))}
            for item in raw_data["records"]
        ]
        return _attach_shares(records, shares_by_date)
    if "raw_financials" in raw_data and isinstance(raw_data["raw_financials"], list):
        records = [
            {**dict(item), "date": _parse_date(dict(item).get("date"))}
            for item in raw_data["raw_financials"]
        ]
        return _attach_shares(records, shares_by_date)
    if "rows" in raw_data and isinstance(raw_data["rows"], list):
        records = [
            {**dict(item), "date": _parse_date(dict(item).get("date"))}
            for item in raw_data["rows"]
        ]
        return _attach_shares(records, shares_by_date)

    if "Financials" in raw_data:
        records = _extract_eodhd_yearly(raw_data["Financials"])
        return _attach_shares(records, shares_by_date)

    if "financials" in raw_data:
        records = _extract_eodhd_yearly(raw_data["financials"])
        return _attach_shares(records, shares_by_date)

    raise ValueError("raw_data must include 'records' or 'Financials'")


def _extract_eodhd_yearly(financials: Mapping[str, Any]) -> list[dict[str, Any]]:
    income = _eodhd_statement_yearly(financials, "Income_Statement")
    balance = _eodhd_statement_yearly(financials, "Balance_Sheet")
    cashflow = _eodhd_statement_yearly(financials, "Cash_Flow")

    dates = sorted(set(income) | set(balance) | set(cashflow))
    return [
        {
            "date": period,
            **income.get(period, {}),
            **balance.get(period, {}),
            **cashflow.get(period, {}),
        }
        for period in dates
    ]


def _extract_outstanding_shares(raw_data: Mapping[str, Any]) -> dict[date, float]:
    shares_block = raw_data.get("outstandingShares")
    if not isinstance(shares_block, Mapping):
        return {}
    annual = shares_block.get("annual")
    if isinstance(annual, Mapping):
        entries = list(annual.values())
    elif isinstance(annual, list):
        entries = annual
    else:
        return {}

    shares_by_date: dict[date, float] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        period = _parse_date(entry.get("dateFormatted")) or _parse_year(entry.get("date"))
        if period is None:
            continue
        shares = _to_float(entry.get("shares"))
        if shares is None:
            shares_mln = _to_float(entry.get("sharesMln"))
            if shares_mln is not None:
                shares = shares_mln * 1_000_000
        if shares is None:
            continue
        shares_by_date[period] = shares
    return shares_by_date


def _attach_shares(
    records: list[dict[str, Any]],
    shares_by_date: Mapping[date, float],
) -> list[dict[str, Any]]:
    if not shares_by_date:
        return records
    for record in records:
        period = record.get("date")
        if not isinstance(period, date):
            continue
        shares = _lookup_shares(shares_by_date, period)
        if shares is not None and "dilutedSharesOutstanding" not in record:
            record["dilutedSharesOutstanding"] = shares
    return records


def _lookup_shares(shares_by_date: Mapping[date, float], period: date) -> float | None:
    if period in shares_by_date:
        return shares_by_date[period]
    matches = [(match_date, shares) for match_date, shares in shares_by_date.items() if match_date.year == period.year]
    if not matches:
        return None
    return max(matches, key=lambda item: item[0])[1]


def _parse_year(value: Any) -> date | None:
    if isinstance(value, str) and value.isdigit() and len(value) == 4:
        try:
            return date(int(value), 12, 31)
        except ValueError:
            return None
    return None


def _eodhd_statement_yearly(
    financials: Mapping[str, Any],
    key: str,
) -> dict[date, dict[str, Any]]:
    statement = financials.get(key, {})
    yearly = statement.get("yearly", {}) if isinstance(statement, Mapping) else {}
    return {
        period: dict(values)
        for period_str, values in yearly.items()
        for period in [_parse_date(period_str)]
        if period is not None
    }


def _build_line_items(
    record: Mapping[str, Any],
    field_map: Mapping[str, tuple[str, ...]],
) -> LineItems:
    period = _parse_date(record.get("date"))
    if period is None:
        raise ValueError("record date is missing or invalid")

    income = _build_income_items(record, field_map)
    balance = _build_balance_items(record, field_map)
    cash_flow = _build_cash_flow_items(record, field_map)

    _assert_accounting_identity(balance)

    return LineItems(period=period, income=income, balance=balance, cash_flow=cash_flow)


def _build_income_items(
    record: Mapping[str, Any],
    field_map: Mapping[str, tuple[str, ...]],
) -> dict[str, float | None]:
    value_of = _value_in(record)
    negative_value_of = _negative_value_in(record)
    revenue = value_of(*field_map["revenue"])
    gross_profit = value_of(*field_map["gross_profit"])
    gross_costs_reported = negative_value_of(*field_map["gross_costs"])
    gross_costs = _checked_value(
        "gross_costs",
        _calculate_gross_costs(revenue, gross_profit),
        gross_costs_reported,
    )

    depreciation = negative_value_of(*field_map["depreciation"])
    amortization = negative_value_of(*field_map["amortization"])
    dep_amort = negative_value_of(*field_map["depreciation_and_amortization"])
    depreciation, amortization = _split_dep_amort(depreciation, amortization, dep_amort)

    operating_income_reported = value_of(*field_map["operating_income"])
    other_operating_expenses = _calculate_other_operating_expenses(
        gross_profit,
        depreciation,
        amortization,
        operating_income_reported,
    )
    operating_income = _checked_value(
        "operating_income",
        _calculate_operating_income(gross_profit, depreciation, amortization, other_operating_expenses),
        operating_income_reported,
    )

    ebitda_reported = value_of("ebitda", "EBITDA")
    ebitda = _checked_value(
        "ebitda",
        _calculate_ebitda(operating_income, depreciation, amortization),
        ebitda_reported,
    )

    interest_income = value_of(*field_map["interest_income"])
    interest_expense = negative_value_of(*field_map["interest_expense"])
    other_non_operating = _calculate_other_non_operating_income(
        operating_income,
        interest_income,
        interest_expense,
        value_of("other_non_operating_income"),
        value_of(*field_map["pre_tax_income"]),
    )

    pre_tax_income = _checked_value(
        "pre_tax_income",
        _calculate_pre_tax_income(
            operating_income,
            interest_income,
            interest_expense,
            other_non_operating,
        ),
        value_of(*field_map["pre_tax_income"]),
    )

    income_tax = negative_value_of(*field_map["income_tax"])
    affiliates_income = value_of(*field_map["affiliates_income"])
    net_income = _checked_value(
        "net_income",
        _calculate_net_income(pre_tax_income, income_tax, affiliates_income),
        value_of(*field_map["net_income"]),
    )

    minorities_expense = negative_value_of(*field_map["minorities_expense"])
    preferred_dividends = negative_value_of(*field_map["preferred_dividends"])
    net_income_common = _calculate_net_income_common(net_income, minorities_expense, preferred_dividends)

    shares_diluted = value_of(*field_map["shares_diluted"])

    return {
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


def _build_balance_items(
    record: Mapping[str, Any],
    field_map: Mapping[str, tuple[str, ...]],
) -> dict[str, float | None]:
    value_of = _value_in(record)
    cash_short_term = value_of(*field_map["cash_short_term_investments"])
    inventory = value_of(*field_map["inventory"])
    receivables = value_of(*field_map["receivables"])
    current_assets = value_of(*field_map["current_assets"])
    other_current_assets = _calculate_other_current_assets(
        current_assets,
        cash_short_term,
        inventory,
        receivables,
    )

    ppe_net = value_of(*field_map["ppe_net"])
    software = value_of(*field_map["software"])
    intangibles = value_of(*field_map["intangibles"])
    investments_lt = value_of(*field_map["long_term_investments"])

    total_assets = value_of(*field_map["total_assets"])
    total_non_current_assets = _calculate_total_non_current_assets(total_assets, current_assets)
    other_non_current_assets = _calculate_other_non_current_assets(
        total_non_current_assets,
        ppe_net,
        software,
        intangibles,
        investments_lt,
    )

    current_liabilities = value_of(*field_map["current_liabilities"])
    accounts_payable = value_of(*field_map["accounts_payable"])
    total_liabilities = value_of(*field_map["total_liabilities"])
    debt_st = value_of(*field_map["debt_short_term"])
    debt_lt = value_of(*field_map["debt_long_term"])

    preferred_stock = value_of(*field_map["preferred_stock"])
    common_equity = value_of(*field_map["common_equity"])
    minority_equity = value_of(*field_map["minority_interest"])
    total_equity = _calculate_total_equity(common_equity, preferred_stock, minority_equity)

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
        "debt_short_term": debt_st,
        "debt_long_term": debt_lt,
        "preferred_stock": preferred_stock,
        "common_equity": common_equity,
        "minority_interest": minority_equity,
        "total_equity": total_equity,
    }


def _build_cash_flow_items(
    record: Mapping[str, Any],
    field_map: Mapping[str, tuple[str, ...]],
) -> dict[str, float | None]:
    value_of = _value_in(record)
    negative_value_of = _negative_value_in(record)
    net_income_cfs = value_of(*field_map["net_income_cfs"])
    depreciation = value_of(*field_map["depreciation"])
    amortization = value_of(*field_map["amortization"])
    dep_amort = value_of(*field_map["depreciation_and_amortization"])
    depreciation, amortization = _split_dep_amort(depreciation, amortization, dep_amort)

    working_cap_change = value_of(*field_map["working_capital_change"])
    cfo_reported = value_of(*field_map["cash_from_operations"])
    other_cfo = _calculate_other_cfo(
        cfo_reported,
        net_income_cfs,
        depreciation,
        amortization,
        working_cap_change,
    )
    cash_from_operations = _checked_value(
        "cash_from_operations",
        _calculate_cash_from_operations(
            net_income_cfs,
            depreciation,
            amortization,
            working_cap_change,
            other_cfo,
        ),
        cfo_reported,
    )

    capex_fixed = negative_value_of(*field_map["capex_fixed"])
    capex_other = negative_value_of(*field_map["capex_other"])
    sale_ppe = value_of(*field_map["sale_ppe"])
    cfi_reported = value_of(*field_map["cash_from_investing"])
    other_cfi = _calculate_other_cfi(cfi_reported, capex_fixed, capex_other, sale_ppe)
    cash_from_investing = _checked_value(
        "cash_from_investing",
        _calculate_cash_from_investing(capex_fixed, capex_other, sale_ppe, other_cfi),
        cfi_reported,
    )

    dividends_paid = negative_value_of(*field_map["dividends_paid"])
    share_purchases = negative_value_of(*field_map["share_purchases"])
    share_sales = value_of(*field_map["share_sales"])
    debt_cf = value_of(*field_map["debt_cash_flow"])
    cff_reported = value_of(*field_map["cash_from_financing"])
    other_cff = _calculate_other_cff(
        cff_reported,
        dividends_paid,
        share_purchases,
        share_sales,
        debt_cf,
    )
    cash_from_financing = _checked_value(
        "cash_from_financing",
        _calculate_cash_from_financing(
            dividends_paid,
            share_purchases,
            share_sales,
            debt_cf,
            other_cff,
        ),
        cff_reported,
    )

    change_in_cash = _calculate_change_in_cash(
        cash_from_operations,
        cash_from_investing,
        cash_from_financing,
    )

    free_cash_flow_reported = _value(record, "freeCashFlow", "free_cash_flow")
    free_cash_flow = _checked_value(
        "free_cash_flow",
        _calculate_free_cash_flow(cash_from_operations, capex_fixed, capex_other),
        free_cash_flow_reported,
    )

    return {
        "net_income": net_income_cfs,
        "depreciation": depreciation,
        "amortization": amortization,
        "working_capital_change": working_cap_change,
        "other_cfo": other_cfo,
        "cash_from_operations": cash_from_operations,
        "capex_fixed": capex_fixed,
        "capex_other": capex_other,
        "sale_ppe": sale_ppe,
        "other_cfi": other_cfi,
        "cash_from_investing": cash_from_investing,
        "dividends_paid": dividends_paid,
        "share_purchases": share_purchases,
        "share_sales": share_sales,
        "debt_cash_flow": debt_cf,
        "other_cff": other_cff,
        "cash_from_financing": cash_from_financing,
        "change_in_cash": change_in_cash,
        "free_cash_flow": free_cash_flow,
    }


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _value(record: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in record:
            return _to_float(record.get(key))
    return None


def _negative_value(record: Mapping[str, Any], *keys: str) -> float | None:
    value = _value(record, *keys)
    if value is None:
        return None
    return -value


def _value_in(record: Mapping[str, Any]):
    return lambda *keys: _value(record, *keys)


def _negative_value_in(record: Mapping[str, Any]):
    return lambda *keys: _negative_value(record, *keys)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _checked_value(name: str, calculated: float | None, reported: float | None) -> float | None:
    if calculated is None:
        return reported
    if reported is None:
        return calculated
    if not isclose(calculated, reported, rel_tol=1e-4, abs_tol=1e-6):
        raise ValueError(f"{name} mismatch: calculated={calculated} reported={reported}")
    return calculated


def _split_dep_amort(
    depreciation: float | None,
    amortization: float | None,
    dep_amort: float | None,
) -> tuple[float | None, float | None]:
    if dep_amort is None:
        return depreciation, amortization
    if depreciation is None and amortization is None:
        return dep_amort, None
    if depreciation is None:
        return dep_amort - (amortization or 0.0), amortization
    if amortization is None:
        return depreciation, dep_amort - depreciation
    return depreciation, amortization


def _calculate_gross_costs(
    revenue: float | None,
    gross_profit: float | None,
) -> float | None:
    if revenue is None or gross_profit is None:
        return None
    return gross_profit - revenue


def _calculate_other_operating_expenses(
    gross_profit: float | None,
    depreciation: float | None,
    amortization: float | None,
    operating_income: float | None,
) -> float | None:
    if gross_profit is None or operating_income is None:
        return None
    depreciation = depreciation or 0.0
    amortization = amortization or 0.0
    return operating_income - gross_profit - depreciation - amortization


def _calculate_operating_income(
    gross_profit: float | None,
    depreciation: float | None,
    amortization: float | None,
    other_operating_expenses: float | None,
) -> float | None:
    if gross_profit is None or other_operating_expenses is None:
        return None
    depreciation = depreciation or 0.0
    amortization = amortization or 0.0
    return gross_profit + depreciation + amortization + other_operating_expenses


def _calculate_other_non_operating_income(
    operating_income: float | None,
    interest_income: float | None,
    interest_expense: float | None,
    reported: float | None,
    pre_tax_income: float | None,
) -> float | None:
    if reported is not None:
        return reported
    if operating_income is None or pre_tax_income is None:
        return None
    interest_income = interest_income or 0.0
    interest_expense = interest_expense or 0.0
    return pre_tax_income - operating_income - interest_income - interest_expense


def _calculate_pre_tax_income(
    operating_income: float | None,
    interest_income: float | None,
    interest_expense: float | None,
    other_non_operating: float | None,
) -> float | None:
    if operating_income is None:
        return None
    interest_income = interest_income or 0.0
    interest_expense = interest_expense or 0.0
    other_non_operating = other_non_operating or 0.0
    return operating_income + interest_income + interest_expense + other_non_operating


def _calculate_net_income(
    pre_tax_income: float | None,
    income_tax: float | None,
    affiliates_income: float | None,
) -> float | None:
    if pre_tax_income is None:
        return None
    income_tax = income_tax or 0.0
    affiliates_income = affiliates_income or 0.0
    return pre_tax_income + income_tax + affiliates_income


def _calculate_net_income_common(
    net_income: float | None,
    minorities_expense: float | None,
    preferred_dividends: float | None,
) -> float | None:
    if net_income is None:
        return None
    minorities_expense = minorities_expense or 0.0
    preferred_dividends = preferred_dividends or 0.0
    return net_income + minorities_expense + preferred_dividends


def _calculate_ebitda(
    operating_income: float | None,
    depreciation: float | None,
    amortization: float | None,
) -> float | None:
    if operating_income is None:
        return None
    depreciation = depreciation or 0.0
    amortization = amortization or 0.0
    return operating_income - depreciation - amortization


def _calculate_other_current_assets(
    current_assets: float | None,
    cash_short_term: float | None,
    inventory: float | None,
    receivables: float | None,
) -> float | None:
    if current_assets is None:
        return None
    cash_short_term = cash_short_term or 0.0
    inventory = inventory or 0.0
    receivables = receivables or 0.0
    return current_assets - cash_short_term - inventory - receivables


def _calculate_total_non_current_assets(
    total_assets: float | None,
    current_assets: float | None,
) -> float | None:
    if total_assets is None or current_assets is None:
        return None
    return total_assets - current_assets


def _calculate_other_non_current_assets(
    total_non_current_assets: float | None,
    ppe_net: float | None,
    software: float | None,
    intangibles: float | None,
    investments_lt: float | None,
) -> float | None:
    if total_non_current_assets is None:
        return None
    ppe_net = ppe_net or 0.0
    software = software or 0.0
    intangibles = intangibles or 0.0
    investments_lt = investments_lt or 0.0
    return total_non_current_assets - ppe_net - software - intangibles - investments_lt


def _calculate_total_equity(
    common_equity: float | None,
    preferred_stock: float | None,
    minority_equity: float | None,
) -> float | None:
    if common_equity is None and preferred_stock is None and minority_equity is None:
        return None
    common_equity = common_equity or 0.0
    preferred_stock = preferred_stock or 0.0
    minority_equity = minority_equity or 0.0
    return common_equity + preferred_stock + minority_equity


def _calculate_other_cfo(
    cash_from_operations: float | None,
    net_income: float | None,
    depreciation: float | None,
    amortization: float | None,
    working_capital_change: float | None,
) -> float | None:
    if cash_from_operations is None:
        return None
    net_income = net_income or 0.0
    depreciation = depreciation or 0.0
    amortization = amortization or 0.0
    working_capital_change = working_capital_change or 0.0
    return (
        cash_from_operations
        - net_income
        - depreciation
        - amortization
        - working_capital_change
    )


def _calculate_cash_from_operations(
    net_income: float | None,
    depreciation: float | None,
    amortization: float | None,
    working_capital_change: float | None,
    other_cfo: float | None,
) -> float | None:
    if net_income is None:
        return None
    depreciation = depreciation or 0.0
    amortization = amortization or 0.0
    working_capital_change = working_capital_change or 0.0
    other_cfo = other_cfo or 0.0
    return net_income + depreciation + amortization + working_capital_change + other_cfo


def _calculate_other_cfi(
    cash_from_investing: float | None,
    capex_fixed: float | None,
    capex_other: float | None,
    sale_ppe: float | None,
) -> float | None:
    if cash_from_investing is None:
        return None
    capex_fixed = capex_fixed or 0.0
    capex_other = capex_other or 0.0
    sale_ppe = sale_ppe or 0.0
    return cash_from_investing - capex_fixed - capex_other - sale_ppe


def _calculate_cash_from_investing(
    capex_fixed: float | None,
    capex_other: float | None,
    sale_ppe: float | None,
    other_cfi: float | None,
) -> float | None:
    if capex_fixed is None and capex_other is None and sale_ppe is None and other_cfi is None:
        return None
    capex_fixed = capex_fixed or 0.0
    capex_other = capex_other or 0.0
    sale_ppe = sale_ppe or 0.0
    other_cfi = other_cfi or 0.0
    return capex_fixed + capex_other + sale_ppe + other_cfi


def _calculate_other_cff(
    cash_from_financing: float | None,
    dividends_paid: float | None,
    share_purchases: float | None,
    share_sales: float | None,
    debt_cash_flow: float | None,
) -> float | None:
    if cash_from_financing is None:
        return None
    dividends_paid = dividends_paid or 0.0
    share_purchases = share_purchases or 0.0
    share_sales = share_sales or 0.0
    debt_cash_flow = debt_cash_flow or 0.0
    return (
        cash_from_financing
        - dividends_paid
        - share_purchases
        - share_sales
        - debt_cash_flow
    )


def _calculate_cash_from_financing(
    dividends_paid: float | None,
    share_purchases: float | None,
    share_sales: float | None,
    debt_cash_flow: float | None,
    other_cff: float | None,
) -> float | None:
    if (
        dividends_paid is None
        and share_purchases is None
        and share_sales is None
        and debt_cash_flow is None
        and other_cff is None
    ):
        return None
    dividends_paid = dividends_paid or 0.0
    share_purchases = share_purchases or 0.0
    share_sales = share_sales or 0.0
    debt_cash_flow = debt_cash_flow or 0.0
    other_cff = other_cff or 0.0
    return dividends_paid + share_purchases + share_sales + debt_cash_flow + other_cff


def _calculate_change_in_cash(
    cash_from_operations: float | None,
    cash_from_investing: float | None,
    cash_from_financing: float | None,
) -> float | None:
    if cash_from_operations is None and cash_from_investing is None and cash_from_financing is None:
        return None
    cash_from_operations = cash_from_operations or 0.0
    cash_from_investing = cash_from_investing or 0.0
    cash_from_financing = cash_from_financing or 0.0
    return cash_from_operations + cash_from_investing + cash_from_financing


def _calculate_free_cash_flow(
    cash_from_operations: float | None,
    capex_fixed: float | None,
    capex_other: float | None,
) -> float | None:
    if cash_from_operations is None:
        return None
    capex_fixed = capex_fixed or 0.0
    capex_other = capex_other or 0.0
    return cash_from_operations + capex_fixed + capex_other


def _assert_accounting_identity(balance: Mapping[str, float | None]) -> None:
    total_assets = balance.get("total_assets")
    total_liabilities = balance.get("total_liabilities")
    total_equity = balance.get("total_equity")
    if total_assets is None or total_liabilities is None or total_equity is None:
        return
    if not isclose(
        total_assets,
        total_liabilities + total_equity,
        rel_tol=1e-4,
        abs_tol=1e-6,
    ):
        raise ValueError(
            "Accounting identity failed: assets != liabilities + equity "
            f"({total_assets} != {total_liabilities} + {total_equity})"
        )
