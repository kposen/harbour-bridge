from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Iterable, Mapping

from sqlalchemy import Engine, create_engine, text

from src.domain.schemas import FinancialModel, LineItems
from src.logic.historic_builder import EODHD_FIELD_MAP


logger = logging.getLogger(__name__)

NEGATIVE_LINE_ITEMS = {
    "gross_costs",
    "depreciation",
    "amortization",
    "interest_expense",
    "income_tax",
    "minorities_expense",
    "preferred_dividends",
    "capex_fixed",
    "capex_other",
    "dividends_paid",
    "share_purchases",
}


def get_engine(db_path: str) -> Engine:
    """Create a SQLAlchemy engine for SQLite.

    Args:
        db_path (str): Filesystem path to the SQLite database.

    Returns:
        Engine: SQLAlchemy engine bound to SQLite.
    """
    return create_engine(f"sqlite:///{db_path}", future=True)


def get_latest_filing_date(engine: Engine, symbol: str) -> date | None:
    """Fetch the most recent filing date for a symbol.

    Args:
        engine (Engine): SQLAlchemy engine for SQL Server.
        symbol (str): Ticker symbol to query.

    Returns:
        date | None: Latest filing date or None if missing.
    """
    query = text(
        """
        SELECT MAX(filing_date) AS latest_filing_date
        FROM financial_facts
        WHERE symbol = :symbol
          AND is_forecast = 0
          AND value_source IN ('reported', 'reported_raw')
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query, {"symbol": symbol}).scalar()
    if isinstance(result, date):
        return result
    return None


def ensure_schema(engine: Engine) -> None:
    """Ensure the financial_facts table exists.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.

    Returns:
        None: Creates schema when missing.
    """
    schema_sql = """
    CREATE TABLE IF NOT EXISTS financial_facts (
        symbol TEXT NOT NULL,
        fiscal_date TEXT NOT NULL,
        filing_date TEXT NOT NULL,
        retrieval_date TEXT NOT NULL,
        period_type TEXT NOT NULL,
        statement TEXT NOT NULL,
        line_item TEXT NOT NULL,
        value_source TEXT NOT NULL,
        value REAL NULL,
        is_forecast INTEGER NOT NULL,
        provider TEXT NOT NULL,
        PRIMARY KEY (
            symbol,
            fiscal_date,
            filing_date,
            retrieval_date,
            period_type,
            statement,
            line_item,
            value_source
        )
    );
    CREATE INDEX IF NOT EXISTS IX_financial_facts_symbol_fiscal
        ON financial_facts (symbol, fiscal_date, period_type);
    CREATE INDEX IF NOT EXISTS IX_financial_facts_retrieval
        ON financial_facts (retrieval_date);
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(schema_sql)


def write_financial_facts(
    engine: Engine,
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    model: FinancialModel,
    filing_dates: Mapping[date, date] | None = None,
    period_type: str = "annual",
    value_source: str = "calculated",
) -> int:
    """Write model line items to the financial_facts table.

    Args:
        engine (Engine): SQLAlchemy engine for SQL Server.
        symbol (str): Ticker symbol for the model.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): When the payload was retrieved.
        model (FinancialModel): Parsed financial model.
        filing_dates (Mapping[date, date] | None): Fiscal date -> filing date map.
        period_type (str): Period type label (e.g., "annual").
        value_source (str): Value source label (e.g., "calculated").

    Returns:
        int: Number of inserted rows.
    """
    rows = list(
        _iter_fact_rows(
            symbol=symbol,
            provider=provider,
            retrieval_date=retrieval_date,
            model=model,
            filing_dates=filing_dates or {},
            period_type=period_type,
            value_source=value_source,
        )
    )
    if not rows:
        return 0
    logger.info("Writing %d fact rows for %s", len(rows), symbol)
    insert_sql = text(
        """
        INSERT INTO financial_facts (
            symbol,
            fiscal_date,
            filing_date,
            retrieval_date,
            period_type,
            statement,
            line_item,
            value_source,
            value,
            is_forecast,
            provider
        )
        VALUES (
            :symbol,
            :fiscal_date,
            :filing_date,
            :retrieval_date,
            :period_type,
            :statement,
            :line_item,
            :value_source,
            :value,
            :is_forecast,
            :provider
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def _iter_fact_rows(
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    model: FinancialModel,
    filing_dates: Mapping[date, date],
    period_type: str,
    value_source: str,
) -> Iterable[dict[str, object]]:
    """Yield fact rows for each line item in the model.

    Args:
        symbol (str): Ticker symbol for the model.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): When the payload was retrieved.
        model (FinancialModel): Parsed financial model.
        filing_dates (Mapping[date, date]): Fiscal date -> filing date map.
        period_type (str): Period type label.
        value_source (str): Value source label.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    history_items = model.history
    all_items = [*model.history, *model.forecast]
    for item in all_items:
        is_forecast = item not in history_items
        filing_date = filing_dates.get(item.period, item.period)
        for statement, values in (
            ("income", item.income),
            ("balance", item.balance),
            ("cash_flow", item.cash_flow),
        ):
            for line_item, value in values.items():
                yield {
                    "symbol": symbol,
                    "fiscal_date": item.period,
                    "filing_date": filing_date,
                    "retrieval_date": retrieval_date,
                    "period_type": period_type,
                    "statement": statement,
                    "line_item": line_item,
                    "value_source": value_source,
                    "value": value,
                    "is_forecast": is_forecast,
                    "provider": provider,
                }


def write_reported_facts(
    engine: Engine,
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
    field_map: Mapping[str, tuple[str, ...]] = EODHD_FIELD_MAP,
) -> int:
    """Write reported provider values (annual + quarterly) to the fact table.

    Args:
        engine (Engine): SQLAlchemy engine for SQL Server.
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.
        field_map (Mapping[str, tuple[str, ...]]): Provider field mapping.

    Returns:
        int: Number of inserted rows.
    """
    rows = list(
        _iter_reported_rows(
            symbol=symbol,
            provider=provider,
            retrieval_date=retrieval_date,
            raw_data=raw_data,
            field_map=field_map,
        )
    )
    if not rows:
        return 0
    logger.info("Writing %d reported fact rows for %s", len(rows), symbol)
    insert_sql = text(
        """
        INSERT INTO financial_facts (
            symbol,
            fiscal_date,
            filing_date,
            retrieval_date,
            period_type,
            statement,
            line_item,
            value_source,
            value,
            is_forecast,
            provider
        )
        VALUES (
            :symbol,
            :fiscal_date,
            :filing_date,
            :retrieval_date,
            :period_type,
            :statement,
            :line_item,
            :value_source,
            :value,
            :is_forecast,
            :provider
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def _iter_reported_rows(
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
    field_map: Mapping[str, tuple[str, ...]],
) -> Iterable[dict[str, object]]:
    """Yield reported provider rows for annual and quarterly periods.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.
        field_map (Mapping[str, tuple[str, ...]]): Provider field mapping.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    financials = raw_data.get("Financials")
    if not isinstance(financials, Mapping):
        return []

    for period_type in ("yearly", "quarterly"):
        for statement, key in (
            ("income", "Income_Statement"),
            ("balance", "Balance_Sheet"),
            ("cash_flow", "Cash_Flow"),
        ):
            statement_block = financials.get(key, {})
            if not isinstance(statement_block, Mapping):
                continue
            period_block = statement_block.get(period_type, {})
            if not isinstance(period_block, Mapping):
                continue
            for fiscal_str, values in period_block.items():
                if not isinstance(values, Mapping):
                    continue
                fiscal_date = _parse_date(fiscal_str)
                if fiscal_date is None:
                    continue
                filing_date = _parse_date(values.get("filing_date")) or fiscal_date
                period_label = "annual" if period_type == "yearly" else "quarterly"
                for line_item, keys in field_map.items():
                    raw_value = _first_value(values, keys)
                    if raw_value is None:
                        continue
                    value = -raw_value if line_item in NEGATIVE_LINE_ITEMS else raw_value
                    yield {
                        "symbol": symbol,
                        "fiscal_date": fiscal_date,
                        "filing_date": filing_date,
                        "retrieval_date": retrieval_date,
                        "period_type": period_label,
                        "statement": statement,
                        "line_item": line_item,
                        "value_source": "reported",
                        "value": value,
                        "is_forecast": False,
                        "provider": provider,
                    }
                for raw_key, raw_value in values.items():
                    numeric_value = _to_float(raw_value)
                    if numeric_value is None:
                        continue
                    yield {
                        "symbol": symbol,
                        "fiscal_date": fiscal_date,
                        "filing_date": filing_date,
                        "retrieval_date": retrieval_date,
                        "period_type": period_label,
                        "statement": statement,
                        "line_item": str(raw_key),
                        "value_source": "reported_raw",
                        "value": numeric_value,
                        "is_forecast": False,
                        "provider": provider,
                    }


def _first_value(values: Mapping[str, object], keys: tuple[str, ...]) -> float | None:
    """Return the first numeric value from a mapping by key preference.

    Args:
        values (Mapping[str, object]): Mapping of raw fields.
        keys (tuple[str, ...]): Candidate keys in order.

    Returns:
        float | None: Parsed numeric value, if present.
    """
    for key in keys:
        if key in values:
            return _to_float(values.get(key))
    return None


def _to_float(value: object) -> float | None:
    """Convert a provider value to float when possible.

    Args:
        value (object): Raw value to convert.

    Returns:
        float | None: Parsed float, if possible.
    """
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


def _parse_date(value: object) -> date | None:
    """Parse a date from ISO string values.

    Args:
        value (object): Raw date value.

    Returns:
        date | None: Parsed date if possible.
    """
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None
