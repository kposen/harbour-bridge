from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Iterable, Mapping

from sqlalchemy import Engine, create_engine, text

from src.domain.schemas import FinancialModel, LineItems
from src.logic.historic_builder import EODHD_FIELD_MAP


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
        value_float REAL NULL,
        value_text TEXT NULL,
        value_type TEXT NOT NULL,
        PRIMARY KEY (symbol, date, period_type, field)
    );
    CREATE INDEX IF NOT EXISTS IX_earnings_symbol_date
        ON earnings (symbol, date);
    CREATE TABLE IF NOT EXISTS holders (
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        totalShares REAL NULL,
        totalAssets REAL NULL,
        currentShares REAL NULL,
        change REAL NULL,
        change_p REAL NULL,
        PRIMARY KEY (symbol, date, name)
    );
    CREATE INDEX IF NOT EXISTS IX_holders_symbol_date
        ON holders (symbol, date);
    CREATE TABLE IF NOT EXISTS insider_transactions (
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        ownerName TEXT NOT NULL,
        transactionDate TEXT NULL,
        transactionCode TEXT NULL,
        transactionAmount REAL NULL,
        transactionPrice REAL NULL,
        transactionAcquiredDisposed TEXT NULL,
        postTransactionAmount REAL NULL,
        secLink TEXT NULL,
        PRIMARY KEY (symbol, date, ownerName)
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
    logger.info("Writing %d market metrics for %s", len(rows), symbol)
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
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def write_earnings(
    engine: Engine,
    symbol: str,
    raw_data: Mapping[str, object],
) -> int:
    """Write earnings payload data to the earnings table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = list(_iter_earnings_rows(symbol, raw_data))
    if not rows:
        return 0
    logger.info("Writing %d earnings rows for %s", len(rows), symbol)
    insert_sql = text(
        """
        INSERT INTO earnings (
            symbol,
            date,
            period_type,
            field,
            value_float,
            value_text,
            value_type
        )
        VALUES (
            :symbol,
            :date,
            :period_type,
            :field,
            :value_float,
            :value_text,
            :value_type
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def write_holders(
    engine: Engine,
    symbol: str,
    raw_data: Mapping[str, object],
) -> int:
    """Write holders payload data to the holders table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _iter_holders_rows(symbol, raw_data)
    if not rows:
        return 0
    logger.info("Writing %d holder rows for %s", len(rows), symbol)
    insert_sql = text(
        """
        INSERT INTO holders (
            symbol,
            date,
            name,
            category,
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
            :totalShares,
            :totalAssets,
            :currentShares,
            :change,
            :change_p
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def write_insider_transactions(
    engine: Engine,
    symbol: str,
    raw_data: Mapping[str, object],
) -> int:
    """Write insider transactions payload data to the insider_transactions table.

    Args:
        engine (Engine): SQLAlchemy engine for SQLite.
        symbol (str): Ticker symbol for the payload.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _iter_insider_rows(symbol, raw_data)
    if not rows:
        return 0
    logger.info("Writing %d insider transactions for %s", len(rows), symbol)
    insert_sql = text(
        """
        INSERT INTO insider_transactions (
            symbol,
            date,
            ownerName,
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
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


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
    logger.info("Writing %d listing rows", len(rows))
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
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
    return len(rows)


def _iter_earnings_rows(
    symbol: str,
    raw_data: Mapping[str, object],
) -> Iterable[dict[str, object]]:
    """Yield earnings rows from the payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    earnings = raw_data.get("Earnings")
    if not isinstance(earnings, Mapping):
        return []
    for branch, period_type in (
        ("History", "quarterly"),
        ("Annual", "annual"),
        ("Trend", "trend"),
    ):
        data = earnings.get(branch)
        if not isinstance(data, Mapping):
            continue
        for _, entry in data.items():
            if not isinstance(entry, Mapping):
                continue
            period = entry.get("date")
            if not isinstance(period, str) or not period.strip():
                continue
            for field, raw_value in entry.items():
                if field in ("reportDate", "date"):
                    continue
                if isinstance(raw_value, (dict, list)):
                    continue
                value_float = _to_float(raw_value)
                if value_float is not None:
                    yield {
                        "symbol": symbol,
                        "date": period,
                        "period_type": period_type,
                        "field": str(field),
                        "value_float": value_float,
                        "value_text": None,
                        "value_type": "float",
                    }
                elif raw_value is not None:
                    yield {
                        "symbol": symbol,
                        "date": period,
                        "period_type": period_type,
                        "field": str(field),
                        "value_float": None,
                        "value_text": str(raw_value),
                        "value_type": "text",
                    }


def _iter_holders_rows(
    symbol: str,
    raw_data: Mapping[str, object],
) -> list[dict[str, object]]:
    """Yield holder rows from the payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    holders = raw_data.get("Holders")
    if not isinstance(holders, Mapping):
        return []
    return [
        {
            "symbol": symbol,
            "date": entry.get("date", "").strip(),
            "name": entry.get("name", "").strip(),
            "category": category,
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
    raw_data: Mapping[str, object],
) -> list[dict[str, object]]:
    """Yield insider transaction rows from the payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    transactions = raw_data.get("InsiderTransactions")
    if not isinstance(transactions, Mapping):
        return []
    rows: list[dict[str, object]] = []
    for _, entry in transactions.items():
        if not isinstance(entry, Mapping):
            continue
        owner = entry.get("ownerName")
        date_str = entry.get("date")
        if not isinstance(owner, str) or not owner.strip():
            continue
        if not isinstance(date_str, str) or not date_str.strip():
            continue
        rows.append(
            {
                "symbol": symbol,
                "date": date_str,
                "ownerName": owner.strip(),
                "transactionDate": entry.get("transactionDate"),
                "transactionCode": entry.get("transactionCode"),
                "transactionAmount": _to_float(entry.get("transactionAmount")),
                "transactionPrice": _to_float(entry.get("transactionPrice")),
                "transactionAcquiredDisposed": entry.get("transactionAcquiredDisposed"),
                "postTransactionAmount": _to_float(entry.get("postTransactionAmount")),
                "secLink": entry.get("secLink"),
            }
        )
    return rows


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
    return [
        {
            "code": entry.get("Code", "").strip(),
            "exchange": entry.get("Exchange", "").strip(),
            "retrieval_date": retrieval_date,
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
    for section in sections:
        data = raw_data.get(section)
        if not isinstance(data, Mapping):
            continue
        metrics = {
            str(metric): raw_value
            for metric, raw_value in data.items()
            if not isinstance(raw_value, (dict, list))
        }
        if section == "General":
            address_data = data.get("AddressData")
            if isinstance(address_data, Mapping):
                for key, value in address_data.items():
                    key_name = str(key)
                    if key_name in metrics:
                        logger.info(
                            "General.AddressData metric '%s' collides with General field '%s'",
                            key_name,
                            key_name,
                        )
                    metrics[key_name] = value
        for metric, raw_value in metrics.items():
            value_float = _to_float(raw_value)
            if value_float is not None:
                yield {
                    "symbol": symbol,
                    "retrieval_date": retrieval_date,
                    "section": section,
                    "metric": metric,
                    "value_float": value_float,
                    "value_text": None,
                    "value_type": "float",
                }
            elif raw_value is not None:
                yield {
                    "symbol": symbol,
                    "retrieval_date": retrieval_date,
                    "section": section,
                    "metric": metric,
                    "value_float": None,
                    "value_text": str(raw_value),
                    "value_type": "text",
                }


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
    history_len = len(model.history)
    all_items = [*model.history, *model.forecast]
    for index, item in enumerate(all_items):
        is_forecast = index >= history_len
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
                    negative_items = STATEMENT_NEGATIVE_LINE_ITEMS.get(statement, set())
                    value = -raw_value if line_item in negative_items else raw_value
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

    outstanding = raw_data.get("outstandingShares")
    if isinstance(outstanding, Mapping):
        for period_type, label in (("annual", "annual"), ("quarterly", "quarterly")):
            block = outstanding.get(period_type)
            if isinstance(block, Mapping):
                entries = block.values()
            elif isinstance(block, list):
                entries = block
            else:
                continue
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                fiscal_date = _parse_date(entry.get("dateFormatted"))
                if fiscal_date is None:
                    continue
                shares = _to_float(entry.get("shares"))
                if shares is None:
                    continue
                yield {
                    "symbol": symbol,
                    "fiscal_date": fiscal_date,
                    "filing_date": fiscal_date,
                    "retrieval_date": retrieval_date,
                    "period_type": label,
                    "statement": "multi_statement",
                    "line_item": "shares",
                    "value_source": "reported",
                    "value": shares,
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
