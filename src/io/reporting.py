from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd


INCOME_ORDER = (
    "revenue",
    "gross_costs",
    "gross_profit",
    "other_operating_expenses",
    "ebitda",
    "depreciation",
    "amortization",
    "operating_income",
    "interest_income",
    "interest_expense",
    "affiliates_income",
    "other_non_operating_income",
    "pre_tax_income",
    "income_tax",
    "net_income",
    "preferred_dividends",
    "minorities_expense",
    "net_income_common",
    "shares_diluted",
)

BALANCE_ORDER = (
    "cash_short_term_investments",
    "inventory",
    "receivables",
    "current_assets",
    "other_current_assets",
    "other_non_current_assets",
    "forecast_net_operating_working_capital",
    "forecast_net_non_operating_working_capital",
    "long_term_investments",
    "ppe_net",
    "software",
    "intangibles",
    "total_non_current_assets",
    "total_assets",
    "accounts_payable",
    "debt_short_term",
    "current_liabilities",
    "debt_long_term",
    "total_liabilities",
    "common_equity",
    "minority_interest",
    "preferred_stock",
    "total_equity",
)

CASH_FLOW_ORDER = (
    "net_income",
    "depreciation",
    "amortization",
    "forecast_depreciation_amortization",
    "working_capital_change",
    "forecast_changes_non_operating_working_capital",
    "other_cfo",
    "cash_from_operations",
    "capex_fixed",
    "sale_ppe",
    "capex_other",
    "forecast_total_capex",
    "other_cfi",
    "cash_from_investing",
    "debt_cash_flow",
    "dividends_paid",
    "share_sales",
    "share_purchases",
    "other_cff",
    "cash_from_financing",
    "change_in_cash",
    "free_cash_flow",
)

NUMBER_FORMAT = "#,##0;[Red](#,##0)"

from src.domain.schemas import FinancialModel, LineItems


logger = logging.getLogger(__name__)


def export_model_to_excel(model: FinancialModel, output_path: Path) -> None:
    """Write a FinancialModel to an Excel workbook.

    Args:
        model (FinancialModel): Model containing history and forecast data.
        output_path (Path): Destination path for the workbook.

    Returns:
        None: Writes an Excel file to disk.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    items = [*model.history, *model.forecast]
    logger.debug("Exporting %d periods to %s", len(items), output_path)
    history_len = len(model.history)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        _statement_frame(items, "income", INCOME_ORDER, history_len).to_excel(
            writer,
            sheet_name="Income statement",
            index_label="Line item",
        )
        _statement_frame(items, "balance", BALANCE_ORDER, history_len).to_excel(
            writer,
            sheet_name="Balance sheet",
            index_label="Line item",
        )
        _statement_frame(items, "cash_flow", CASH_FLOW_ORDER, history_len).to_excel(
            writer,
            sheet_name="Cash flow statement",
            index_label="Line item",
        )
        _format_workbook(writer, history_len)


def _statement_frame(
    items: Iterable[LineItems],
    section: str,
    order: Iterable[str],
    history_len: int,
) -> pd.DataFrame:
    """Build a statement DataFrame with line items as rows and years as columns.

    Args:
        items (Iterable[LineItems]): Line items to render.
        section (str): Statement selector ("income", "balance", "cash_flow").
        order (Iterable[str]): Ordered line item keys to include.
        history_len (int): Count of historical periods.

    Returns:
        pd.DataFrame: Statement values indexed by line item name.
    """
    items_list = list(items)
    periods = [item.period.isoformat() for item in items_list]
    data = [_section_map(item, section) for item in items_list]
    keys = [key for key in order if any(key in mapping for mapping in data)]
    rows = {key: [mapping.get(key) for mapping in data] for key in keys}
    frame = pd.DataFrame(rows, index=periods).transpose()
    frame.index.name = "Line item"
    actual_flags = ["A" if idx < history_len else "F" for idx in range(len(periods))]
    flag_row = pd.DataFrame([actual_flags], index=["Actual/Forecast"], columns=periods)
    return pd.concat([flag_row, frame])


def _section_map(item: LineItems, section: str) -> dict[str, float | None]:
    """Select the statement mapping from a LineItems object.

    Args:
        item (LineItems): The line items container.
        section (str): Statement selector ("income", "balance", "cash_flow").

    Returns:
        dict[str, float | None]: Statement mapping for the section.
    """
    if section == "income":
        return dict(item.income)
    if section == "balance":
        return dict(item.balance)
    if section == "cash_flow":
        return dict(item.cash_flow)
    raise ValueError(f"Unknown statement section: {section}")


def _format_workbook(writer: pd.ExcelWriter, history_len: int) -> None:
    """Apply formatting for numbers, labels, and gridlines.

    Args:
        writer (pd.ExcelWriter): Excel writer with workbook/worksheets.
        history_len (int): Count of historical periods.

    Returns:
        None: Mutates workbook formatting.
    """
    for sheet in writer.sheets.values():
        sheet.sheet_view.showGridLines = False
        # Apply number format to data cells (excluding header and label rows).
        for row in sheet.iter_rows(min_row=3, min_col=2):
            for cell in row:
                cell.number_format = NUMBER_FORMAT
        # Left align line item descriptions.
        for cell in sheet.iter_rows(min_row=2, max_col=1):
            for item in cell:
                item.alignment = item.alignment.copy(horizontal="left")
