from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

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
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        _statement_frame(items, "income").to_excel(writer, sheet_name="Income statement")
        _statement_frame(items, "balance").to_excel(writer, sheet_name="Balance sheet")
        _statement_frame(items, "cash_flow").to_excel(writer, sheet_name="Cash flow statement")


def _statement_frame(items: Iterable[LineItems], section: str) -> pd.DataFrame:
    """Build a statement DataFrame with line items as rows and years as columns.

    Args:
        items (Iterable[LineItems]): Line items to render.
        section (str): Statement selector ("income", "balance", "cash_flow").

    Returns:
        pd.DataFrame: Statement values indexed by line item name.
    """
    items_list = list(items)
    years = [item.period.year for item in items_list]
    data = [_section_map(item, section) for item in items_list]
    keys = sorted({key for mapping in data for key in mapping})
    rows = {key: [mapping.get(key) for mapping in data] for key in keys}
    return pd.DataFrame(rows, index=years).transpose()


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
