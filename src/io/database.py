from __future__ import annotations

import logging
from datetime import date, datetime
from functools import partial
from itertools import chain
from typing import Iterable, Mapping

from math import isclose

from toolz.itertoolz import mapcat
from sqlalchemy import Engine, create_engine, text

from src.domain.schemas import FinancialModel, LineItems
from src.logic.historic_builder import EODHD_FIELD_MAP
from src.config import get_database_tolerances


logger = logging.getLogger(__name__)

STATEMENT_NEGATIVE_LINE_ITEMS = {
    "income": {
        "gross_costs",
        "depreciation",
        "amortization",
        "interest_expense",
        "income_tax",
        "minorities_expense",
        "preferred_dividends",
    },
    "cash_flow": {
        "capex_fixed",
        "capex_other",
        "dividends_paid",
        "share_purchases",
    },
    "balance": set(),
}

RETRIEVAL_COLUMN = "retrieval_date"


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
        engine (Engine): SQLAlchemy engine for SQLite.
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
          AND statement IN ('income', 'balance', 'cash_flow')
          AND value_source IN ('reported', 'reported_raw')
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query, {"symbol": symbol}).scalar()
    if isinstance(result, datetime):
        return result.date()
    if isinstance(result, date):
        return result
    if isinstance(result, str):
        try:
            return date.fromisoformat(result)
        except ValueError:
            return None
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
    CREATE TABLE IF NOT EXISTS market_metrics (
        symbol TEXT NOT NULL,
        retrieval_date TEXT NOT NULL,
        section TEXT NOT NULL,
        metric TEXT NOT NULL,
        value_float REAL NULL,
        value_text TEXT NULL,
        value_type TEXT NOT NULL,
        PRIMARY KEY (symbol, retrieval_date, section, metric)
    );
    CREATE INDEX IF NOT EXISTS IX_market_metrics_symbol
        ON market_metrics (symbol, retrieval_date);
    CREATE TABLE IF NOT EXISTS earnings (
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        period_type TEXT NOT NULL,
        field TEXT NOT NULL,
        retrieval_date TEXT NOT NULL,
        value_float REAL NULL,
        value_text TEXT NULL,
        value_type TEXT NOT NULL,
        PRIMARY KEY (symbol, date, period_type, field, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_earnings_symbol_date
        ON earnings (symbol, date);
    CREATE TABLE IF NOT EXISTS holders (
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        retrieval_date TEXT NOT NULL,
        totalShares REAL NULL,
        totalAssets REAL NULL,
        currentShares REAL NULL,
        change REAL NULL,
        change_p REAL NULL,
        PRIMARY KEY (symbol, date, name, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_holders_symbol_date
        ON holders (symbol, date);
    CREATE TABLE IF NOT EXISTS insider_transactions (
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        ownerName TEXT NOT NULL,
        retrieval_date TEXT NOT NULL,
        transactionDate TEXT NULL,
        transactionCode TEXT NULL,
        transactionAmount REAL NULL,
        transactionPrice REAL NULL,
        transactionAcquiredDisposed TEXT NULL,
        postTransactionAmount REAL NULL,
        secLink TEXT NULL,
        PRIMARY KEY (symbol, date, ownerName, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_insider_transactions_symbol_date
        ON insider_transactions (symbol, date);
    CREATE TABLE IF NOT EXISTS listings (
        code TEXT NOT NULL,
        exchange TEXT NOT NULL,
        retrieval_date TEXT NOT NULL,
        primary_ticker TEXT NOT NULL,
        name TEXT NULL,
        PRIMARY KEY (code, exchange, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_listings_primary_ticker
        ON listings (primary_ticker, retrieval_date);
    """
    with engine.begin() as conn:
        for statement in (stmt.strip() for stmt in schema_sql.split(";")):
            if statement:
                conn.exec_driver_sql(statement)


def write_market_metrics(
    engine: Engine,
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> int:
    """Write market metrics sections (Highlights, Valuation, etc.) to SQLite.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = list(_iter_market_metrics(symbol, retrieval_date, raw_data))
    if not rows:
        return 0
    insert_sql = text(
        """
        INSERT INTO market_metrics (
            symbol,
            retrieval_date,
            section,
            metric,
            value_float,
            value_text,
            value_type
        )
        VALUES (
            :symbol,
            :retrieval_date,
            :section,
            :metric,
            :value_float,
            :value_text,
            :value_type
        )
        """
    )
    match_columns = ("symbol", "section", "metric")
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="market_metrics",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d market metrics for %s", len(rows_to_insert), symbol)
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def write_earnings(
    engine: Engine,
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> int:
    """Write earnings payload data to the earnings table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = list(_iter_earnings_rows(symbol, retrieval_date, raw_data))
    if not rows:
        return 0
    insert_sql = text(
        """
        INSERT INTO earnings (
            symbol,
            date,
            period_type,
            field,
            retrieval_date,
            value_float,
            value_text,
            value_type
        )
        VALUES (
            :symbol,
            :date,
            :period_type,
            :field,
            :retrieval_date,
            :value_float,
            :value_text,
            :value_type
        )
        """
    )
    match_columns = ("symbol", "date", "period_type", "field")
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="earnings",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d earnings rows for %s", len(rows_to_insert), symbol)
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def write_holders(
    engine: Engine,
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> int:
    """Write holders payload data to the holders table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _iter_holders_rows(symbol, retrieval_date, raw_data)
    if not rows:
        return 0
    insert_sql = text(
        """
        INSERT INTO holders (
            symbol,
            date,
            name,
            category,
            retrieval_date,
            totalShares,
            totalAssets,
            currentShares,
            change,
            change_p
        )
        VALUES (
            :symbol,
            :date,
            :name,
            :category,
            :retrieval_date,
            :totalShares,
            :totalAssets,
            :currentShares,
            :change,
            :change_p
        )
        """
    )
    match_columns = ("symbol", "date", "name")
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="holders",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d holder rows for %s", len(rows_to_insert), symbol)
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def write_insider_transactions(
    engine: Engine,
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> int:
    """Write insider transactions payload data to the insider_transactions table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _iter_insider_rows(symbol, retrieval_date, raw_data)
    if not rows:
        return 0
    insert_sql = text(
        """
        INSERT INTO insider_transactions (
            symbol,
            date,
            ownerName,
            retrieval_date,
            transactionDate,
            transactionCode,
            transactionAmount,
            transactionPrice,
            transactionAcquiredDisposed,
            postTransactionAmount,
            secLink
        )
        VALUES (
            :symbol,
            :date,
            :ownerName,
            :retrieval_date,
            :transactionDate,
            :transactionCode,
            :transactionAmount,
            :transactionPrice,
            :transactionAcquiredDisposed,
            :postTransactionAmount,
            :secLink
        )
        """
    )
    match_columns = ("symbol", "date", "ownerName")
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="insider_transactions",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d insider transactions for %s", len(rows_to_insert), symbol)
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def write_listings(
    engine: Engine,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> int:
    """Write listing relationships from General.Listings to the listings table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _iter_listings_rows(retrieval_date, raw_data)
    if not rows:
        return 0
    insert_sql = text(
        """
        INSERT INTO listings (
            code,
            exchange,
            retrieval_date,
            primary_ticker,
            name
        )
        VALUES (
            :code,
            :exchange,
            :retrieval_date,
            :primary_ticker,
            :name
        )
        """
    )
    match_columns = ("code", "exchange")
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="listings",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d listing rows", len(rows_to_insert))
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def _iter_earnings_rows(
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> Iterable[dict[str, object]]:
    """Yield earnings rows from the payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    earnings = raw_data.get("Earnings")
    if not isinstance(earnings, Mapping):
        return []
    retrieval_stamp = retrieval_date.isoformat()
    branches = (("History", "quarterly"), ("Annual", "annual"), ("Trend", "trend"))

    def branch_rows(pair: tuple[str, str]) -> Iterable[dict[str, object]]:
        """Build earnings rows for a branch tuple.

        Args:
            pair (tuple[str, str]): Branch key and period type.

        Returns:
            Iterable[dict[str, object]]: Row dictionaries for insertion.
        """
        branch, period_type = pair
        return _iter_earnings_branch(
            symbol=symbol,
            retrieval_stamp=retrieval_stamp,
            period_type=period_type,
            data=earnings.get(branch),
        )

    return mapcat(branch_rows, branches)


def _iter_earnings_branch(
    symbol: str,
    retrieval_stamp: str,
    period_type: str,
    data: object,
) -> Iterable[dict[str, object]]:
    """Yield earnings rows for a specific earnings branch.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        period_type (str): Period type label ("annual", "quarterly", "trend").
        data (object): Branch payload to parse.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    if not isinstance(data, Mapping):
        return []
    entry_rows = partial(
        _iter_earnings_entry_rows,
        symbol,
        retrieval_stamp,
        period_type,
    )
    entries = (entry for entry in data.values() if isinstance(entry, Mapping))
    return mapcat(entry_rows, entries)


def _iter_earnings_entry_rows(
    symbol: str,
    retrieval_stamp: str,
    period_type: str,
    entry: Mapping[str, object],
) -> list[dict[str, object]]:
    """Yield earnings rows for a single entry in a branch.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        period_type (str): Period type label.
        entry (Mapping[str, object]): Entry payload to parse.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    period = entry.get("date")
    if not isinstance(period, str) or not period.strip():
        return []

    def row_for_field(field: str, raw_value: object) -> dict[str, object] | None:
        """Build an earnings row for a single field/value pair.

        Args:
            field (str): Field name for the entry.
            raw_value (object): Raw field value.

        Returns:
            dict[str, object] | None: Row dictionary or None when invalid.
        """
        if field in ("reportDate", "date"):
            return None
        if isinstance(raw_value, (dict, list)):
            return None
        base = {
            "symbol": symbol,
            "date": period,
            "period_type": period_type,
            "field": str(field),
            "retrieval_date": retrieval_stamp,
        }
        return _typed_metric_row(base, raw_value)

    rows = [
        row
        for field, raw_value in entry.items()
        for row in [row_for_field(str(field), raw_value)]
        if row is not None
    ]
    return rows


def _iter_holders_rows(
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> list[dict[str, object]]:
    """Yield holder rows from the payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    holders = raw_data.get("Holders")
    if not isinstance(holders, Mapping):
        return []
    retrieval_stamp = retrieval_date.isoformat()
    return [
        {
            "symbol": symbol,
            "date": entry.get("date", "").strip(),
            "name": entry.get("name", "").strip(),
            "category": category,
            "retrieval_date": retrieval_stamp,
            "totalShares": _to_float(entry.get("totalShares")),
            "totalAssets": _to_float(entry.get("totalAssets")),
            "currentShares": _to_float(entry.get("currentShares")),
            "change": _to_float(entry.get("change")),
            "change_p": _to_float(entry.get("change_p")),
        }
        for category in ("Institutions", "Funds")
        for group in [holders.get(category)]
        if isinstance(group, Mapping)
        for entry in group.values()
        if isinstance(entry, Mapping)
        if isinstance(entry.get("name"), str)
        if entry.get("name", "").strip()
        if isinstance(entry.get("date"), str)
        if entry.get("date", "").strip()
    ]


def _iter_insider_rows(
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> list[dict[str, object]]:
    """Yield insider transaction rows from the payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    transactions = raw_data.get("InsiderTransactions")
    if not isinstance(transactions, Mapping):
        return []
    retrieval_stamp = retrieval_date.isoformat()
    row_builder = partial(_insider_row, symbol, retrieval_stamp)
    return [row for row in map(row_builder, transactions.values()) if row is not None]


def _insider_row(
    symbol: str,
    retrieval_stamp: str,
    entry: object,
) -> dict[str, object] | None:
    """Build a row for a single insider transaction entry.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        entry (object): Raw entry payload.

    Returns:
        dict[str, object] | None: Row dictionary or None when invalid.
    """
    if not isinstance(entry, Mapping):
        return None
    owner = entry.get("ownerName")
    date_str = entry.get("date")
    if not isinstance(owner, str) or not owner.strip():
        return None
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    return {
        "symbol": symbol,
        "date": date_str,
        "ownerName": owner.strip(),
        "retrieval_date": retrieval_stamp,
        "transactionDate": entry.get("transactionDate"),
        "transactionCode": entry.get("transactionCode"),
        "transactionAmount": _to_float(entry.get("transactionAmount")),
        "transactionPrice": _to_float(entry.get("transactionPrice")),
        "transactionAcquiredDisposed": entry.get("transactionAcquiredDisposed"),
        "postTransactionAmount": _to_float(entry.get("postTransactionAmount")),
        "secLink": entry.get("secLink"),
    }


def _iter_listings_rows(
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> list[dict[str, object]]:
    """Yield listing relationship rows from the payload.

    Args:
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    general = raw_data.get("General")
    if not isinstance(general, Mapping):
        return []
    primary_ticker = general.get("PrimaryTicker")
    listings = general.get("Listings")
    if not isinstance(primary_ticker, str) or not primary_ticker.strip():
        return []
    if not isinstance(listings, Mapping):
        return []
    retrieval_stamp = retrieval_date.isoformat()
    return [
        {
            "code": entry.get("Code", "").strip(),
            "exchange": entry.get("Exchange", "").strip(),
            "retrieval_date": retrieval_stamp,
            "primary_ticker": primary_ticker.strip(),
            "name": entry.get("Name"),
        }
        for entry in listings.values()
        if isinstance(entry, Mapping)
        if isinstance(entry.get("Code"), str)
        if entry.get("Code", "").strip()
        if isinstance(entry.get("Exchange"), str)
        if entry.get("Exchange", "").strip()
    ]


def _iter_market_metrics(
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> Iterable[dict[str, object]]:
    """Yield market metric rows from supported payload sections.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    sections = (
        "General",
        "Highlights",
        "Valuation",
        "ShareStats",
        "SharesStats",
        "Technicals",
        "AnalystRatings",
        "SplitsDividends",
    )
    retrieval_stamp = retrieval_date.isoformat()

    def section_rows(section: str) -> Iterable[dict[str, object]]:
        """Build metric rows for a section.

        Args:
            section (str): Section name to parse.

        Returns:
            Iterable[dict[str, object]]: Row dictionaries for insertion.
        """
        return _market_section_rows(symbol, retrieval_stamp, section, raw_data.get(section))

    return mapcat(section_rows, sections)


def _market_section_rows(
    symbol: str,
    retrieval_stamp: str,
    section: str,
    data: object,
) -> list[dict[str, object]]:
    """Build metric rows for a single payload section.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        section (str): Payload section name.
        data (object): Raw section payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    if not isinstance(data, Mapping):
        return []
    metrics = {
        str(metric): raw_value
        for metric, raw_value in data.items()
        if not isinstance(raw_value, (dict, list))
    }
    if section == "General":
        address_data = data.get("AddressData")
        if isinstance(address_data, Mapping):
            collisions = [str(key) for key in address_data.keys() if str(key) in metrics]
            for key_name in collisions:
                logger.info(
                    "General.AddressData metric '%s' collides with General field '%s'",
                    key_name,
                    key_name,
                )
            metrics = {
                **metrics,
                **{str(key): value for key, value in address_data.items()},
            }
    base = {
        "symbol": symbol,
        "retrieval_date": retrieval_stamp,
        "section": section,
    }
    rows = [
        row
        for metric, raw_value in metrics.items()
        for row in [_typed_metric_row({**base, "metric": metric}, raw_value)]
        if row is not None
    ]
    return rows


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
        engine (Engine): SQLAlchemy engine for SQLite.
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
    match_columns = (
        "symbol",
        "fiscal_date",
        "filing_date",
        "period_type",
        "statement",
        "line_item",
        "value_source",
        "provider",
        "is_forecast",
    )
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="financial_facts",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d fact rows for %s", len(rows_to_insert), symbol)
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


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
    history_len = len(model.history)
    retrieval_stamp = retrieval_date.isoformat()
    all_items = [*model.history, *model.forecast]
    items_with_flags = (
        (item, index >= history_len) for index, item in enumerate(all_items)
    )
    return (
        {
            "symbol": symbol,
            "fiscal_date": item.period.isoformat(),
            "filing_date": filing_dates.get(item.period, item.period).isoformat(),
            "retrieval_date": retrieval_stamp,
            "period_type": period_type,
            "statement": statement,
            "line_item": line_item,
            "value_source": value_source,
            "value": value,
            "is_forecast": int(is_forecast),
            "provider": provider,
        }
        for item, is_forecast in items_with_flags
        for statement, values in _statement_maps(item)
        for line_item, value in values.items()
    )


def _statement_maps(item: LineItems) -> tuple[tuple[str, Mapping[str, float | None]], ...]:
    """Return statement/value mappings for a LineItems instance.

    Args:
        item (LineItems): LineItems instance to inspect.

    Returns:
        tuple[tuple[str, Mapping[str, float | None]], ...]: Statement/value pairs.
    """
    return (
        ("income", item.income),
        ("balance", item.balance),
        ("cash_flow", item.cash_flow),
    )


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
        engine (Engine): SQLAlchemy engine for SQLite.
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
    match_columns = (
        "symbol",
        "fiscal_date",
        "filing_date",
        "period_type",
        "statement",
        "line_item",
        "value_source",
        "provider",
        "is_forecast",
    )
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="financial_facts",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d reported fact rows for %s", len(rows_to_insert), symbol)
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


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

    retrieval_stamp = retrieval_date.isoformat()
    period_types = (("yearly", "annual"), ("quarterly", "quarterly"))
    statement_keys = (
        ("income", "Income_Statement"),
        ("balance", "Balance_Sheet"),
        ("cash_flow", "Cash_Flow"),
    )
    statement_rows = chain.from_iterable(
        _iter_reported_statement_rows(
            symbol=symbol,
            provider=provider,
            retrieval_stamp=retrieval_stamp,
            period_label=period_label,
            statement=statement,
            period_block=_period_block(financials, key, period_key),
            field_map=field_map,
        )
        for period_key, period_label in period_types
        for statement, key in statement_keys
    )
    outstanding_rows = _iter_outstanding_rows(
        symbol=symbol,
        provider=provider,
        retrieval_stamp=retrieval_stamp,
        outstanding=raw_data.get("outstandingShares"),
    )
    return chain(statement_rows, outstanding_rows)


def _period_block(
    financials: Mapping[str, object],
    key: str,
    period_key: str,
) -> Mapping[str, object] | None:
    """Return a statement period block from the financials payload.

    Args:
        financials (Mapping[str, object]): Financials payload mapping.
        key (str): Statement key (e.g., Income_Statement).
        period_key (str): Period key ("yearly" or "quarterly").

    Returns:
        Mapping[str, object] | None: Period block mapping when available.
    """
    statement_block = financials.get(key)
    if not isinstance(statement_block, Mapping):
        return None
    period_block = statement_block.get(period_key)
    if not isinstance(period_block, Mapping):
        return None
    return period_block


def _iter_reported_statement_rows(
    symbol: str,
    provider: str,
    retrieval_stamp: str,
    period_label: str,
    statement: str,
    period_block: Mapping[str, object] | None,
    field_map: Mapping[str, tuple[str, ...]],
) -> Iterable[dict[str, object]]:
    """Yield reported rows for a single statement and period block.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        period_label (str): Period type label ("annual" or "quarterly").
        statement (str): Statement identifier ("income", "balance", "cash_flow").
        period_block (Mapping[str, object] | None): Period mapping for the statement.
        field_map (Mapping[str, tuple[str, ...]]): Provider field mapping.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    if period_block is None:
        return []
    return chain.from_iterable(
        _iter_reported_period_rows(
            symbol=symbol,
            provider=provider,
            retrieval_stamp=retrieval_stamp,
            period_label=period_label,
            statement=statement,
            fiscal_str=fiscal_str,
            values=values,
            field_map=field_map,
        )
        for fiscal_str, values in period_block.items()
        if isinstance(values, Mapping)
    )


def _iter_reported_period_rows(
    symbol: str,
    provider: str,
    retrieval_stamp: str,
    period_label: str,
    statement: str,
    fiscal_str: str,
    values: Mapping[str, object],
    field_map: Mapping[str, tuple[str, ...]],
) -> list[dict[str, object]]:
    """Yield reported rows for a single fiscal period.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        period_label (str): Period type label ("annual" or "quarterly").
        statement (str): Statement identifier ("income", "balance", "cash_flow").
        fiscal_str (str): Fiscal date string.
        values (Mapping[str, object]): Statement values for the period.
        field_map (Mapping[str, tuple[str, ...]]): Provider field mapping.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    fiscal_date = _parse_date(fiscal_str)
    if fiscal_date is None:
        return []
    filing_date = _parse_date(values.get("filing_date")) or fiscal_date
    base = {
        "symbol": symbol,
        "fiscal_date": fiscal_date.isoformat(),
        "filing_date": filing_date.isoformat(),
        "retrieval_date": retrieval_stamp,
        "period_type": period_label,
        "statement": statement,
        "is_forecast": 0,
        "provider": provider,
    }
    negative_items = STATEMENT_NEGATIVE_LINE_ITEMS.get(statement, set())
    mapped_rows = [
        {
            **base,
            "line_item": line_item,
            "value_source": "reported",
            "value": -raw_value if line_item in negative_items else raw_value,
        }
        for line_item, keys in field_map.items()
        for raw_value in [_first_value(values, keys)]
        if raw_value is not None
    ]
    raw_rows = [
        {
            **base,
            "line_item": str(raw_key),
            "value_source": "reported_raw",
            "value": numeric_value,
        }
        for raw_key, raw_value in values.items()
        for numeric_value in [_to_float(raw_value)]
        if numeric_value is not None
    ]
    return [*mapped_rows, *raw_rows]


def _iter_outstanding_rows(
    symbol: str,
    provider: str,
    retrieval_stamp: str,
    outstanding: object,
) -> list[dict[str, object]]:
    """Yield reported rows for outstanding shares.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        outstanding (object): Outstanding shares payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    if not isinstance(outstanding, Mapping):
        return []
    period_types = (("annual", "annual"), ("quarterly", "quarterly"))

    def period_rows(pair: tuple[str, str]) -> list[dict[str, object]]:
        """Build outstanding shares rows for a period type.

        Args:
            pair (tuple[str, str]): Outstanding period key and label.

        Returns:
            list[dict[str, object]]: Row dictionaries for insertion.
        """
        period_key, label = pair
        return _outstanding_period_rows(
            symbol=symbol,
            provider=provider,
            retrieval_stamp=retrieval_stamp,
            period_label=label,
            block=outstanding.get(period_key),
        )

    return list(mapcat(period_rows, period_types))


def _outstanding_period_rows(
    symbol: str,
    provider: str,
    retrieval_stamp: str,
    period_label: str,
    block: object,
) -> list[dict[str, object]]:
    """Yield outstanding shares rows for a specific period type.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_stamp (str): Retrieval timestamp as ISO string.
        period_label (str): Period type label ("annual" or "quarterly").
        block (object): Outstanding shares block (mapping or list).

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    entries = _normalize_outstanding_block(block)
    rows = [
        {
            "symbol": symbol,
            "fiscal_date": fiscal_date.isoformat(),
            "filing_date": fiscal_date.isoformat(),
            "retrieval_date": retrieval_stamp,
            "period_type": period_label,
            "statement": "multi_statement",
            "line_item": "shares",
            "value_source": "reported",
            "value": shares,
            "is_forecast": 0,
            "provider": provider,
        }
        for entry in entries
        if isinstance(entry, Mapping)
        for fiscal_date in [_parse_date(entry.get("dateFormatted"))]
        if fiscal_date is not None
        for shares in [_to_float(entry.get("shares"))]
        if shares is not None
    ]
    return rows


def _normalize_outstanding_block(block: object) -> Iterable[Mapping[str, object]]:
    """Normalize outstanding shares blocks into an iterable of entries.

    Args:
        block (object): Outstanding shares block (mapping, list, or other).

    Returns:
        Iterable[Mapping[str, object]]: Iterable of entries.
    """
    if isinstance(block, Mapping):
        return block.values()
    if isinstance(block, list):
        return block
    return []


def _filter_versioned_rows(
    conn,
    table: str,
    rows: list[dict[str, object]],
    match_columns: tuple[str, ...],
    retrieval_column: str = RETRIEVAL_COLUMN,
) -> list[dict[str, object]]:
    """Filter rows to those that should be inserted as new versions.

    Args:
        conn (Connection): SQLAlchemy connection for querying.
        table (str): Table name for version checks.
        rows (list[dict[str, object]]): Candidate rows for insertion.
        match_columns (tuple[str, ...]): Columns defining a record identity.
        retrieval_column (str): Column used for versioning.

    Returns:
        list[dict[str, object]]: Rows that should be inserted.
    """
    if not rows:
        return []
    rel_tol, abs_tol = get_database_tolerances()
    where_clause = " AND ".join(f"{column} = :{column}" for column in match_columns)
    query = text(
        f"""
        SELECT *
        FROM {table}
        WHERE {where_clause}
        ORDER BY {retrieval_column} DESC
        LIMIT 1
        """
    )
    def _row_if_new(row: dict[str, object]) -> dict[str, object] | None:
        """Return the row when it should be inserted as a new version.

        Args:
            row (dict[str, object]): Candidate row for insertion.

        Returns:
            dict[str, object] | None: Row to insert, or None when unchanged.
        """
        match_params = {column: row.get(column) for column in match_columns}
        existing = conn.execute(query, match_params).mappings().first()
        if existing is None:
            return row
        compare_columns = [
            column
            for column in row.keys()
            if column not in match_columns and column != retrieval_column
        ]
        return None if _rows_equal(existing, row, compare_columns, rel_tol, abs_tol) else row

    return [row for row in map(_row_if_new, rows) if row is not None]


def _rows_equal(
    existing: Mapping[str, object],
    incoming: Mapping[str, object],
    compare_columns: list[str],
    rel_tol: float,
    abs_tol: float,
) -> bool:
    """Compare existing and incoming rows with tolerance for numeric values.

    Args:
        existing (Mapping[str, object]): Existing row mapping.
        incoming (Mapping[str, object]): Incoming row mapping.
        compare_columns (list[str]): Columns to compare for equality.
        rel_tol (float): Relative tolerance for numeric comparisons.
        abs_tol (float): Absolute tolerance for numeric comparisons.

    Returns:
        bool: True if rows are equivalent by column comparison.
    """
    return all(
        _values_equal(
            existing.get(column),
            incoming.get(column),
            rel_tol=rel_tol,
            abs_tol=abs_tol,
        )
        for column in compare_columns
    )


def _values_equal(value: object, other: object, rel_tol: float, abs_tol: float) -> bool:
    """Compare two values with numeric normalization and tolerance.

    Args:
        value (object): First value to compare.
        other (object): Second value to compare.
        rel_tol (float): Relative tolerance for numeric comparisons.
        abs_tol (float): Absolute tolerance for numeric comparisons.

    Returns:
        bool: True when values are equivalent.
    """
    if value is None and other is None:
        return True
    if value is None or other is None:
        return False
    value_float = _parse_float(value)
    other_float = _parse_float(other)
    if value_float is not None and other_float is not None:
        return isclose(value_float, other_float, rel_tol=rel_tol, abs_tol=abs_tol)
    return _normalize_text(value) == _normalize_text(other)


def _parse_float(value: object) -> float | None:
    """Attempt to parse a float from a value.

    Args:
        value (object): Value to parse.

    Returns:
        float | None: Parsed float when possible.
    """
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


def _normalize_text(value: object) -> str:
    """Normalize a value into a comparable string.

    Args:
        value (object): Value to normalize.

    Returns:
        str: Normalized string representation.
    """
    return str(value).strip()


def _typed_metric_row(
    base: Mapping[str, object],
    raw_value: object,
) -> dict[str, object] | None:
    """Build a typed metric row from a raw value.

    Args:
        base (Mapping[str, object]): Base row fields to include.
        raw_value (object): Raw value to parse.

    Returns:
        dict[str, object] | None: Row dictionary or None when empty.
    """
    typed = _typed_value(raw_value)
    if typed is None:
        return None
    value_type, value_float, value_text = typed
    return {
        **dict(base),
        "value_float": value_float,
        "value_text": value_text,
        "value_type": value_type,
    }


def _typed_value(raw_value: object) -> tuple[str, float | None, str | None] | None:
    """Normalize raw values into typed representations.

    Args:
        raw_value (object): Raw value to parse.

    Returns:
        tuple[str, float | None, str | None] | None: Type label and parsed values.
    """
    value_float = _to_float(raw_value)
    if value_float is not None:
        return "float", value_float, None
    if raw_value is None:
        return None
    return "text", None, str(raw_value)


def _first_value(values: Mapping[str, object], keys: tuple[str, ...]) -> float | None:
    """Return the first numeric value from a mapping by key preference.

    Args:
        values (Mapping[str, object]): Mapping of raw fields.
        keys (tuple[str, ...]): Candidate keys in order.

    Returns:
        float | None: Parsed numeric value, if present.
    """
    return next((_to_float(values.get(key)) for key in keys if key in values), None)


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
