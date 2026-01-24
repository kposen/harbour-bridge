from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import UTC, date, datetime
from functools import partial
from itertools import chain
from typing import Iterable, Mapping

from math import isclose

from toolz.itertoolz import mapcat
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import Connection

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

PRICE_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "open": ("open",),
    "high": ("high",),
    "low": ("low",),
    "close": ("close",),
    "adjusted_close": ("adjusted_close", "adjustedClose", "adj_close", "adjClose"),
    "volume": ("volume",),
}

RETRIEVAL_COLUMN = "retrieval_date"
SCRATCH_TABLE = "pipeline_scratch"
EXCHANGES_TABLE = "exchanges"
PRIMARY_LISTING_MAP_TABLE = "primary_listing_map"
UNIVERSE_TABLE = "universe"
EXCHANGE_LIST_COLUMNS = (
    "name",
    "operating_mic",
    "country",
    "currency",
    "country_iso2",
    "country_iso3",
)
SHARE_UNIVERSE_COLUMNS = (
    "symbol",
    "code",
    "name",
    "country",
    "exchange",
    "currency",
    "type",
    "isin",
)
EXCHANGE_LIST_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "name": ("Name", "name"),
    "operating_mic": ("OperatingMIC", "operating_mic", "operatingMIC"),
    "country": ("Country", "country"),
    "currency": ("Currency", "currency"),
    "country_iso2": ("CountryISO2", "country_iso2", "countryISO2"),
    "country_iso3": ("CountryISO3", "country_iso3", "countryISO3"),
}
MARKET_METRIC_TYPES: dict[str, str] = {
    "200DayMA": "float",
    "50DayMA": "float",
    "52WeekHigh": "float",
    "52WeekLow": "float",
    "Address": "text",
    "Beta": "float",
    "BookValue": "float",
    "Buy": "float",
    "CIK": "text",
    "CUSIP": "text",
    "City": "text",
    "Code": "text",
    "Country": "text",
    "CountryISO": "text",
    "CountryName": "text",
    "CurrencyCode": "text",
    "CurrencyName": "text",
    "CurrencySymbol": "text",
    "Description": "text",
    "DilutedEpsTTM": "float",
    "DividendDate": "date",
    "DividendShare": "float",
    "DividendYield": "float",
    "EBITDA": "float",
    "EPSEstimateCurrentQuarter": "float",
    "EPSEstimateCurrentYear": "float",
    "EPSEstimateNextQuarter": "float",
    "EPSEstimateNextYear": "float",
    "EarningsShare": "float",
    "EmployerIdNumber": "text",
    "EnterpriseValue": "float",
    "EnterpriseValueEbitda": "float",
    "EnterpriseValueRevenue": "float",
    "ExDividendDate": "date",
    "Exchange": "text",
    "FiscalYearEnd": "text",
    "ForwardAnnualDividendRate": "float",
    "ForwardAnnualDividendYield": "float",
    "ForwardPE": "float",
    "FullTimeEmployees": "float",
    "GicGroup": "text",
    "GicIndustry": "text",
    "GicSector": "text",
    "GicSubIndustry": "text",
    "GrossProfitTTM": "float",
    "Hold": "float",
    "HomeCategory": "text",
    "IPODate": "date",
    "ISIN": "text",
    "Industry": "text",
    "InternationalDomestic": "text",
    "IsDelisted": "float",
    "LEI": "text",
    "LastSplitDate": "date",
    "LastSplitFactor": "text",
    "LogoURL": "text",
    "MarketCapitalization": "float",
    "MarketCapitalizationMln": "float",
    "MostRecentQuarter": "date",
    "Name": "text",
    "OpenFigi": "text",
    "OperatingMarginTTM": "float",
    "PEGRatio": "float",
    "PERatio": "float",
    "PayoutRatio": "float",
    "PercentInsiders": "float",
    "PercentInstitutions": "float",
    "Phone": "text",
    "PriceBookMRQ": "float",
    "PriceSalesTTM": "float",
    "PrimaryTicker": "text",
    "ProfitMargin": "float",
    "QuarterlyEarningsGrowthYOY": "float",
    "QuarterlyRevenueGrowthYOY": "float",
    "Rating": "float",
    "ReturnOnAssetsTTM": "float",
    "ReturnOnEquityTTM": "float",
    "RevenuePerShareTTM": "float",
    "RevenueTTM": "float",
    "Sector": "text",
    "Sell": "float",
    "SharesFloat": "float",
    "SharesOutstanding": "float",
    "SharesShort": "float",
    "SharesShortPriorMonth": "float",
    "ShortPercent": "float",
    "ShortPercentFloat": "float",
    "ShortRatio": "float",
    "State": "text",
    "Street": "text",
    "StrongBuy": "float",
    "StrongSell": "float",
    "TargetPrice": "float",
    "TrailingPE": "float",
    "Type": "text",
    "UpdatedAt": "date",
    "WallStreetTargetPrice": "float",
    "WebURL": "text",
    "ZIP": "text",
}
MARKET_METRIC_COLUMNS = tuple(MARKET_METRIC_TYPES.keys())


def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for Postgres.

    Args:
        database_url (str): SQLAlchemy database URL (Postgres DSN).

    Returns:
        Engine: SQLAlchemy engine bound to Postgres.
    """
    return create_engine(database_url, future=True)


def get_latest_filing_date(engine: Engine, symbol: str) -> date | None:
    """Fetch the most recent filing date for a symbol.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        symbol (str): Ticker symbol to query.

    Returns:
        date | None: Latest filing date or None if missing.
    """
    query = text(
        """
        SELECT MAX(filing_date) AS latest_filing_date
        FROM financial_facts
        WHERE symbol = :symbol
          AND is_forecast = FALSE
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


def get_latest_price_date(engine: Engine, symbol: str, provider: str) -> date | None:
    """Fetch the most recent price date for a symbol/provider pair.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        symbol (str): Ticker symbol to query.
        provider (str): Provider name (e.g., "EODHD").

    Returns:
        date | None: Latest price date or None if missing.
    """
    query = text(
        """
        SELECT MAX(date) AS latest_date
        FROM prices
        WHERE symbol = :symbol
          AND provider = :provider
        """
    )
    with engine.begin() as conn:
        result = conn.execute(query, {"symbol": symbol, "provider": provider}).scalar()
    return _parse_date(result)


def get_exchange_codes(engine: Engine) -> list[str]:
    """Return the latest exchange codes with complete metadata.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.

    Returns:
        list[str]: Exchange codes from the most recent rows per exchange.
    """
    query = text(
        f"""
        SELECT
            code,
            {", ".join(EXCHANGE_LIST_COLUMNS)}
        FROM {EXCHANGES_TABLE}
        WHERE (code, {RETRIEVAL_COLUMN}) IN (
            SELECT code, MAX({RETRIEVAL_COLUMN})
            FROM {EXCHANGES_TABLE}
            GROUP BY code
        )
        ORDER BY code
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()
    if not rows:
        logger.info("No exchanges rows available for share universe refresh")
        return []
    annotated = [
        {
            "code": _normalize_exchange_code(row),
            "missing_fields": _exchange_missing_fields(row),
        }
        for row in rows
    ]
    valid_codes = [
        code
        for item in annotated
        for code in [item["code"]]
        if code is not None and (not item["missing_fields"] or code == "FOREX")
    ]
    invalid = [
        {
            "code": item["code"] or "<missing>",
            "missing_fields": item["missing_fields"],
        }
        for item in annotated
        if item["code"] is None
        or (item["missing_fields"] and item["code"] != "FOREX")
    ]
    missing_code_count = sum(1 for item in annotated if item["code"] is None)
    forex_override = sum(
        1
        for item in annotated
        if item["code"] == "FOREX" and item["missing_fields"]
    )
    logger.info(
        "Exchanges filter: total=%d eligible=%d skipped=%d",
        len(rows),
        len(valid_codes),
        len(invalid),
    )
    if forex_override:
        logger.info("Exchanges filter override: kept FOREX with missing fields")
    if missing_code_count:
        logger.debug("Skipped %d exchanges with missing codes", missing_code_count)
    if invalid:
        missing_counts = Counter(
            chain.from_iterable(
                item["missing_fields"]
                for item in invalid
                if item["missing_fields"]
            )
        )
        logger.debug(
            "Skipped exchanges due to incomplete metadata (sample): %s",
            invalid[:25],
        )
        if missing_counts:
            logger.debug("Missing exchange field counts: %s", dict(missing_counts))
    return valid_codes


def ensure_schema(engine: Engine) -> None:
    """Ensure the Postgres schema exists.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.

    Returns:
        None: Creates schema when missing.
    """
    if engine.dialect.name != "postgresql":
        raise ValueError(f"Unsupported database dialect: {engine.dialect.name}")
    schema_sql = _postgres_schema_sql()
    with engine.begin() as conn:
        for statement in (stmt.strip() for stmt in schema_sql.split(";")):
            if statement:
                conn.exec_driver_sql(statement)


def _postgres_schema_sql() -> str:
    """Return Postgres DDL for application tables."""
    market_columns_sql = _market_metric_columns_sql()
    return f"""
    CREATE TABLE IF NOT EXISTS financial_facts (
        symbol TEXT NOT NULL,
        fiscal_date DATE NOT NULL,
        filing_date DATE NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        period_type TEXT NOT NULL,
        statement TEXT NOT NULL,
        line_item TEXT NOT NULL,
        value_source TEXT NOT NULL,
        value DOUBLE PRECISION NULL,
        is_forecast BOOLEAN NOT NULL,
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
        retrieval_date TIMESTAMPTZ NOT NULL,
{market_columns_sql},
        PRIMARY KEY (symbol, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_market_metrics_symbol
        ON market_metrics (symbol, retrieval_date);
    CREATE TABLE IF NOT EXISTS holders (
        symbol TEXT NOT NULL,
        date DATE NOT NULL,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        totalShares DOUBLE PRECISION NULL,
        totalAssets DOUBLE PRECISION NULL,
        currentShares DOUBLE PRECISION NULL,
        change DOUBLE PRECISION NULL,
        change_p DOUBLE PRECISION NULL,
        PRIMARY KEY (symbol, date, name, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_holders_symbol_date
        ON holders (symbol, date);
    CREATE TABLE IF NOT EXISTS insider_transactions (
        symbol TEXT NOT NULL,
        date DATE NOT NULL,
        ownerName TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        transactionDate DATE NULL,
        transactionCode TEXT NULL,
        transactionAmount DOUBLE PRECISION NULL,
        transactionPrice DOUBLE PRECISION NULL,
        transactionAcquiredDisposed TEXT NULL,
        postTransactionAmount DOUBLE PRECISION NULL,
        secLink TEXT NULL,
        PRIMARY KEY (symbol, date, ownerName, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_insider_transactions_symbol_date
        ON insider_transactions (symbol, date);
    CREATE TABLE IF NOT EXISTS primary_listing_map (
        code TEXT NOT NULL,
        exchange TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        primary_ticker TEXT NOT NULL,
        name TEXT NULL,
        PRIMARY KEY (code, exchange, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_primary_listing_map_primary_ticker
        ON primary_listing_map (primary_ticker, retrieval_date);
    CREATE TABLE IF NOT EXISTS prices (
        symbol TEXT NOT NULL,
        date DATE NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        provider TEXT NOT NULL,
        open DOUBLE PRECISION NULL,
        high DOUBLE PRECISION NULL,
        low DOUBLE PRECISION NULL,
        close DOUBLE PRECISION NULL,
        adjusted_close DOUBLE PRECISION NULL,
        volume DOUBLE PRECISION NULL,
        PRIMARY KEY (symbol, date, retrieval_date, provider)
    );
    CREATE INDEX IF NOT EXISTS IX_prices_symbol_date
        ON prices (symbol, date);
    CREATE TABLE IF NOT EXISTS exchanges (
        retrieval_date TIMESTAMPTZ NOT NULL,
        code TEXT NOT NULL,
        name TEXT NULL,
        operating_mic TEXT NULL,
        country TEXT NULL,
        currency TEXT NULL,
        country_iso2 TEXT NULL,
        country_iso3 TEXT NULL,
        PRIMARY KEY (retrieval_date, code)
    );
    CREATE INDEX IF NOT EXISTS IX_exchanges_code
        ON exchanges (code);
    CREATE TABLE IF NOT EXISTS universe (
        symbol TEXT NOT NULL,
        code TEXT NOT NULL,
        name TEXT NULL,
        country TEXT NULL,
        exchange TEXT NOT NULL,
        currency TEXT NULL,
        type TEXT NULL,
        isin TEXT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (symbol, exchange, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_universe_symbol
        ON universe (symbol, exchange);
    CREATE TABLE IF NOT EXISTS earnings (
        symbol TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        date DATE NOT NULL,
        fiscal_date DATE NULL,
        before_after_market TEXT NULL,
        currency TEXT NULL,
        actual DOUBLE PRECISION NULL,
        estimate DOUBLE PRECISION NULL,
        difference DOUBLE PRECISION NULL,
        percent DOUBLE PRECISION NULL,
        PRIMARY KEY (symbol, date, retrieval_date)
    );
    CREATE TABLE IF NOT EXISTS dividends (
        symbol TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        date DATE NOT NULL,
        currency TEXT NULL,
        amount DOUBLE PRECISION NULL,
        period TEXT NULL,
        declaration_date DATE NULL,
        record_date DATE NULL,
        payment_date DATE NULL,
        PRIMARY KEY (symbol, date, retrieval_date)
    );
    CREATE TABLE IF NOT EXISTS splits (
        symbol TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
        date DATE NOT NULL,
        optionable BOOLEAN NULL,
        old_shares DOUBLE PRECISION NULL,
        new_shares DOUBLE PRECISION NULL,
        PRIMARY KEY (symbol, date, retrieval_date)
    );
    """


def _market_metric_columns_sql() -> str:
    """Build SQL column definitions for market metrics."""
    def column_sql(metric: str) -> str:
        """Build SQL for a single market metrics column."""
        metric_type = MARKET_METRIC_TYPES.get(metric, "text")
        if metric_type == "float":
            sql_type = "DOUBLE PRECISION"
        elif metric_type == "date":
            sql_type = "DATE"
        else:
            sql_type = "TEXT"
        return f'        "{metric}" {sql_type} NULL'

    return ",\n".join(column_sql(metric) for metric in MARKET_METRIC_COLUMNS)




def write_market_metrics(
    engine: Engine,
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> int:
    """Write market metrics sections (Highlights, Valuation, etc.) to Postgres.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    row = _market_metrics_row(symbol, retrieval_date, raw_data)
    if row is None:
        return 0
    columns = ["symbol", "retrieval_date", *MARKET_METRIC_COLUMNS]
    param_map = {column: _metric_param_name(column) for column in columns}
    insert_sql = text(
        f"""
        INSERT INTO market_metrics (
            {", ".join(_quote_identifier(column) for column in columns)}
        )
        VALUES (
            {", ".join(f":{param_map[column]}" for column in columns)}
        )
        """
    )
    rows = [row]
    match_columns = ("symbol",)
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="market_metrics",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        param_rows = [
            {param_map[column]: row.get(column) for column in columns}
            for row in rows_to_insert
        ]
        logger.info("Writing %d market metrics rows for %s", len(param_rows), symbol)
        conn.execute(insert_sql, param_rows)
    return len(rows_to_insert)


def write_prices(
    engine: Engine,
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    raw_data: object,
) -> int:
    """Write end-of-day price rows into the prices table.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (object): Raw provider payload for prices.

    Returns:
        int: Number of inserted rows.
    """
    rows = list(_iter_price_rows(symbol, provider, retrieval_date, raw_data))
    if not rows:
        return 0
    insert_sql = text(
        """
        INSERT INTO prices (
            symbol,
            date,
            retrieval_date,
            provider,
            open,
            high,
            low,
            close,
            adjusted_close,
            volume
        )
        VALUES (
            :symbol,
            :date,
            :retrieval_date,
            :provider,
            :open,
            :high,
            :low,
            :close,
            :adjusted_close,
            :volume
        )
        """
    )
    match_columns = ("symbol", "date", "provider")
    with engine.begin() as conn:
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table="prices",
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d price rows for %s", len(rows_to_insert), symbol)
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
        engine (Engine): SQLAlchemy engine for Postgres.
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
        engine (Engine): SQLAlchemy engine for Postgres.
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
    """Write listing relationships from General.Listings to the primary_listing_map table.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (Mapping[str, object]): Raw provider payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _iter_listings_rows(retrieval_date, raw_data)
    if not rows:
        return 0
    insert_sql = text(
        f"""
        INSERT INTO {PRIMARY_LISTING_MAP_TABLE} (
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
            table=PRIMARY_LISTING_MAP_TABLE,
            rows=rows,
            match_columns=match_columns,
        )
        if not rows_to_insert:
            return 0
        logger.info("Writing %d listing rows", len(rows_to_insert))
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def write_exchange_list(
    engine: Engine,
    retrieval_date: datetime,
    payload: object | None,
) -> int:
    """Write exchange list rows to the exchanges table.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw exchange list payload.

    Returns:
        int: Number of inserted rows.
    """
    rows = _exchange_rows(retrieval_date, payload)
    if not rows:
        logger.debug("No exchange list rows parsed from payload")
        return 0
    logger.debug("Prepared %d exchange list rows for insertion", len(rows))
    columns = ["code", RETRIEVAL_COLUMN, *EXCHANGE_LIST_COLUMNS]
    insert_sql = text(
        f"""
        INSERT INTO {EXCHANGES_TABLE} (
            {", ".join(columns)}
        )
        VALUES (
            {", ".join(f":{column}" for column in columns)}
        )
        """
    )
    with engine.begin() as conn:
        rows = [{column: row.get(column) for column in columns} for row in rows]
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table=EXCHANGES_TABLE,
            rows=rows,
            match_columns=("code",),
            retrieval_column=RETRIEVAL_COLUMN,
        )
        logger.debug(
            "Exchange list rows: %d candidate, %d new after dedup",
            len(rows),
            len(rows_to_insert),
        )
        if not rows_to_insert:
            logger.debug("No new exchange list rows to insert after deduplication")
            return 0
        logger.info("Writing %d exchange list rows", len(rows_to_insert))
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def write_share_universe(
    engine: Engine,
    retrieval_date: datetime,
    payload: object | None,
) -> int:
    """Write share universe rows to the universe table.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw share universe payload for one exchange.

    Returns:
        int: Number of inserted rows.
    """
    rows = _share_universe_rows(retrieval_date, payload)
    if not rows:
        logger.debug("No share universe rows parsed from payload")
        return 0
    logger.debug("Prepared %d share universe rows for insertion", len(rows))
    columns = [*SHARE_UNIVERSE_COLUMNS, RETRIEVAL_COLUMN]
    insert_sql = text(
        f"""
        INSERT INTO {UNIVERSE_TABLE} (
            {", ".join(columns)}
        )
        VALUES (
            {", ".join(f":{column}" for column in columns)}
        )
        """
    )
    with engine.begin() as conn:
        rows = [{column: row.get(column) for column in columns} for row in rows]
        rows_to_insert = _filter_versioned_rows(
            conn=conn,
            table=UNIVERSE_TABLE,
            rows=rows,
            match_columns=("symbol", "exchange"),
            retrieval_column=RETRIEVAL_COLUMN,
        )
        logger.debug(
            "Share universe rows: %d candidate, %d new after dedup",
            len(rows),
            len(rows_to_insert),
        )
        if not rows_to_insert:
            logger.debug("No new share universe rows to insert after deduplication")
            return 0
        logger.info("Writing %d share universe rows", len(rows_to_insert))
        conn.execute(insert_sql, rows_to_insert)
    return len(rows_to_insert)


def run_database_preflight(engine: Engine) -> None:
    """Run database connectivity and scratch-table tests.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.

    Returns:
        None: Raises RuntimeError when critical checks fail.
    """
    logger.info("Running database preflight checks")
    _assert_db_connectivity(engine)
    _assert_scratch_table_roundtrip(engine)
    logger.info("Database preflight checks passed")


def _assert_db_connectivity(engine: Engine) -> None:
    """Ensure the database connection is healthy."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
    except Exception as exc:
        raise RuntimeError("Database connectivity check failed") from exc
    if result != 1:
        raise RuntimeError("Database connectivity check returned unexpected result")


def _assert_scratch_table_roundtrip(engine: Engine) -> None:
    """Ensure the scratch table supports write/read/delete operations."""
    token = f"preflight-{datetime.now(UTC).isoformat()}"
    created_at = datetime.now(UTC)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {SCRATCH_TABLE} (
                        token TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {SCRATCH_TABLE} (token, created_at)
                    VALUES (:token, :created_at)
                    """
                ),
                {"token": token, "created_at": created_at},
            )
            fetched = conn.execute(
                text(f"SELECT token FROM {SCRATCH_TABLE} WHERE token = :token"),
                {"token": token},
            ).scalar()
            if fetched != token:
                raise RuntimeError("Scratch table read verification failed")
            conn.execute(
                text(f"DELETE FROM {SCRATCH_TABLE} WHERE token = :token"),
                {"token": token},
            )
            remaining = conn.execute(
                text(f"SELECT COUNT(*) FROM {SCRATCH_TABLE} WHERE token = :token"),
                {"token": token},
            ).scalar()
            if remaining not in (0, None):
                raise RuntimeError("Scratch table delete verification failed")
    except Exception as exc:
        raise RuntimeError("Scratch table round-trip failed") from exc


def write_corporate_actions_calendar(
    engine: Engine,
    retrieval_date: datetime,
    earnings_payload: object | None,
    splits_payload: object | None,
    dividends_payloads: list[object],
) -> int:
    """Write upcoming earnings, splits, and dividends to corporate actions tables.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        retrieval_date (datetime): When the payloads were retrieved.
        earnings_payload (object | None): Raw earnings calendar payload.
        splits_payload (object | None): Raw splits calendar payload.
        dividends_payloads (list[object]): Raw dividends calendar payloads.

    Returns:
        int: Number of inserted rows.
    """
    earnings_rows = list(_iter_earnings_calendar_rows(retrieval_date, earnings_payload))
    splits_rows = list(_iter_split_calendar_rows(retrieval_date, splits_payload))
    dividends_rows = [
        row
        for payload in dividends_payloads
        for row in _iter_dividend_calendar_rows(retrieval_date, payload)
    ]
    logger.debug(
        "Prepared corporate actions rows: earnings=%d splits=%d dividends=%d",
        len(earnings_rows),
        len(splits_rows),
        len(dividends_rows),
    )
    if not earnings_rows and not splits_rows and not dividends_rows:
        logger.debug("No corporate actions calendar rows parsed from payloads")
        return 0
    earnings_insert = text(
        """
        INSERT INTO earnings (
            symbol,
            retrieval_date,
            date,
            fiscal_date,
            before_after_market,
            currency,
            actual,
            estimate,
            difference,
            percent
        )
        VALUES (
            :symbol,
            :retrieval_date,
            :date,
            :fiscal_date,
            :before_after_market,
            :currency,
            :actual,
            :estimate,
            :difference,
            :percent
        )
        """
    )
    dividends_insert = text(
        """
        INSERT INTO dividends (
            symbol,
            retrieval_date,
            date,
            currency,
            amount,
            period,
            declaration_date,
            record_date,
            payment_date
        )
        VALUES (
            :symbol,
            :retrieval_date,
            :date,
            :currency,
            :amount,
            :period,
            :declaration_date,
            :record_date,
            :payment_date
        )
        """
    )
    splits_insert = text(
        """
        INSERT INTO splits (
            symbol,
            retrieval_date,
            date,
            optionable,
            old_shares,
            new_shares
        )
        VALUES (
            :symbol,
            :retrieval_date,
            :date,
            :optionable,
            :old_shares,
            :new_shares
        )
        """
    )
    inserted = 0
    with engine.begin() as conn:
        if earnings_rows:
            rows_to_insert = _filter_versioned_rows(
                conn=conn,
                table="earnings",
                rows=earnings_rows,
                match_columns=("symbol", "date"),
            )
            logger.debug(
                "Earnings calendar rows: %d candidate, %d new after dedup",
                len(earnings_rows),
                len(rows_to_insert),
            )
            if rows_to_insert:
                logger.info("Writing %d upcoming earnings calendar rows", len(rows_to_insert))
                conn.execute(earnings_insert, rows_to_insert)
                inserted += len(rows_to_insert)
            else:
                logger.debug("No new earnings calendar rows after deduplication")
        if splits_rows:
            rows_to_insert = _filter_versioned_rows(
                conn=conn,
                table="splits",
                rows=splits_rows,
                match_columns=("symbol", "date"),
            )
            logger.debug(
                "Splits calendar rows: %d candidate, %d new after dedup",
                len(splits_rows),
                len(rows_to_insert),
            )
            if rows_to_insert:
                logger.info("Writing %d upcoming splits calendar rows", len(rows_to_insert))
                conn.execute(splits_insert, rows_to_insert)
                inserted += len(rows_to_insert)
            else:
                logger.debug("No new splits calendar rows after deduplication")
        if dividends_rows:
            rows_to_insert = _filter_versioned_rows(
                conn=conn,
                table="dividends",
                rows=dividends_rows,
                match_columns=("symbol", "date"),
            )
            logger.debug(
                "Dividends calendar rows: %d candidate, %d new after dedup",
                len(dividends_rows),
                len(rows_to_insert),
            )
            if rows_to_insert:
                logger.info("Writing %d upcoming dividends calendar rows", len(rows_to_insert))
                conn.execute(dividends_insert, rows_to_insert)
                inserted += len(rows_to_insert)
            else:
                logger.debug("No new dividends calendar rows after deduplication")
    return inserted


def _exchange_rows(
    retrieval_date: datetime,
    payload: object | None,
) -> list[dict[str, object]]:
    """Build exchange list rows from the payload.

    Args:
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw exchange list payload.

    Returns:
        list[dict[str, object]]: Exchange list rows keyed by static columns.
    """
    if payload is None:
        return []
    entries = _exchange_entries(payload)
    if not entries:
        logger.debug("Exchange list payload contained no usable entries")
        return []
    rows: list[dict[str, object]] = []
    for entry in entries:
        code = _normalize_exchange_code(entry)
        if code is None:
            continue
        row = {
            RETRIEVAL_COLUMN: retrieval_date,
            "code": code,
        }
        for column, keys in EXCHANGE_LIST_FIELD_MAP.items():
            row[column] = _normalize_exchange_value(_first_present(entry, keys))
        rows.append(row)
    logger.debug("Parsed %d exchange list entries into %d rows", len(entries), len(rows))
    return rows


def _share_universe_rows(
    retrieval_date: datetime,
    payload: object | None,
) -> list[dict[str, object]]:
    """Build share universe rows from a single exchange payload.

    Args:
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw share universe payload.

    Returns:
        list[dict[str, object]]: Share universe rows keyed by columns.
    """
    if payload is None:
        return []
    entries = _share_universe_entries(payload)
    if not entries:
        logger.debug("Share universe payload contained no usable entries")
        return []
    normalized = [
        (
            entry,
            _normalize_share_code(_first_present(entry, ("Code", "code", "CODE"))),
            _normalize_share_code(_first_present(entry, ("Exchange", "exchange"))),
        )
        for entry in entries
    ]
    missing_code = sum(1 for _, code, _ in normalized if code is None)
    missing_exchange = sum(1 for _, _, exchange in normalized if exchange is None)
    rows = [
        {
            RETRIEVAL_COLUMN: retrieval_date,
            "symbol": f"{code}.{exchange}",
            "code": code,
            "name": _normalize_share_value(_first_present(entry, ("Name", "name"))),
            "country": _normalize_share_value(_first_present(entry, ("Country", "country"))),
            "exchange": exchange,
            "currency": _normalize_share_value(_first_present(entry, ("Currency", "currency"))),
            "type": _normalize_share_value(_first_present(entry, ("Type", "type"))),
            "isin": _normalize_share_value(_first_present(entry, ("Isin", "ISIN", "isin"))),
        }
        for entry, code, exchange in normalized
        if code is not None and exchange is not None
    ]
    if missing_code or missing_exchange:
        skipped = [
            {"code": code, "exchange": exchange}
            for _, code, exchange in normalized
            if code is None or exchange is None
        ]
        logger.debug(
            "Share universe entries skipped due to missing identifiers: %d",
            len(skipped),
        )
        logger.debug(
            "Skipped share universe entries (sample): %s",
            skipped[:25],
        )
    logger.debug(
        "Share universe entry summary: total=%d valid=%d missing_code=%d missing_exchange=%d",
        len(entries),
        len(rows),
        missing_code,
        missing_exchange,
    )
    logger.debug("Parsed %d share universe entries into %d rows", len(entries), len(rows))
    return rows


def _share_universe_entries(payload: object) -> list[Mapping[str, object]]:
    """Normalize share universe payloads into a list of entries."""
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, Mapping)]
    if isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, list):
            return [entry for entry in data if isinstance(entry, Mapping)]
        return [entry for entry in payload.values() if isinstance(entry, Mapping)]
    return []


def _exchange_entries(payload: object) -> list[Mapping[str, object]]:
    """Normalize exchange list payloads into a list of entries."""
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, Mapping)]
    if isinstance(payload, Mapping):
        exchanges = payload.get("exchanges")
        data = payload.get("data")
        if isinstance(exchanges, list):
            return [entry for entry in exchanges if isinstance(entry, Mapping)]
        if isinstance(data, list):
            return [entry for entry in data if isinstance(entry, Mapping)]
        return [entry for entry in payload.values() if isinstance(entry, Mapping)]
    return []


def _normalize_exchange_code(entry: Mapping[str, object]) -> str | None:
    """Extract and normalize the exchange code from a payload entry."""
    raw_code = _first_present(entry, ("Code", "code", "CODE"))
    if isinstance(raw_code, str):
        stripped = raw_code.strip()
        if not stripped:
            return None
        upper = stripped.upper()
        return None if upper == "UNKNOWN" else upper
    return None


def get_symbols_with_history(
    engine: Engine,
    provider: str,
    period_type: str = "annual",
) -> list[str]:
    """Return symbols that have reported financial facts in the database.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        provider (str): Provider name (e.g., "EODHD").
        period_type (str): Period type label to filter on.

    Returns:
        list[str]: Distinct symbols with reported facts.
    """
    query = text(
        """
        SELECT DISTINCT symbol
        FROM financial_facts
        WHERE provider = :provider
          AND period_type = :period_type
          AND value_source = 'reported'
          AND is_forecast = FALSE
        ORDER BY symbol
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(query, {"provider": provider, "period_type": period_type}).fetchall()
    return [row[0] for row in rows if isinstance(row[0], str)]


def load_historic_model_from_db(
    engine: Engine,
    symbol: str,
    provider: str,
    period_type: str = "annual",
) -> tuple[FinancialModel, dict[date, date]]:
    """Load historical facts from the database into a FinancialModel.

    Args:
        engine (Engine): SQLAlchemy engine for Postgres.
        symbol (str): Ticker symbol to load.
        provider (str): Provider name (e.g., "EODHD").
        period_type (str): Period type label (e.g., "annual").

    Returns:
        tuple[FinancialModel, dict[date, date]]: Model plus filing-date map.
    """
    logger.info("Loading historical facts for %s from database", symbol)
    query = text(
        """
        SELECT fiscal_date, filing_date, statement, line_item, value
        FROM (
            SELECT
                fiscal_date,
                filing_date,
                statement,
                line_item,
                value,
                ROW_NUMBER() OVER (
                    PARTITION BY fiscal_date, statement, line_item
                    ORDER BY retrieval_date DESC
                ) AS rn
            FROM financial_facts
            WHERE symbol = :symbol
              AND provider = :provider
              AND period_type = :period_type
              AND value_source = 'reported'
              AND is_forecast = FALSE
        ) latest
        WHERE rn = 1
        ORDER BY fiscal_date
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(
            query,
            {"symbol": symbol, "provider": provider, "period_type": period_type},
        ).mappings().all()
    if not rows:
        logger.info("No reported facts found for %s", symbol)
        return FinancialModel(history=[], forecast=[]), {}
    items_by_date: dict[date, dict[str, dict[str, float | None]]] = {}
    filing_dates: dict[date, date] = {}
    for row in rows:
        fiscal_date = row.get("fiscal_date")
        if not isinstance(fiscal_date, date):
            continue
        statement = row.get("statement")
        line_item = row.get("line_item")
        value = _to_float(row.get("value"))
        filing_date = _parse_date(row.get("filing_date"))
        if fiscal_date not in items_by_date:
            items_by_date[fiscal_date] = {
                "income": {},
                "balance": {},
                "cash_flow": {},
            }
        if filing_date is not None:
            existing = filing_dates.get(fiscal_date)
            if existing is None or filing_date > existing:
                filing_dates[fiscal_date] = filing_date
        if statement == "multi_statement" and line_item == "shares":
            income = items_by_date[fiscal_date]["income"]
            if "shares_diluted" not in income and value is not None:
                income["shares_diluted"] = value
            continue
        if statement not in items_by_date[fiscal_date]:
            logger.debug("Skipping unsupported statement %s for %s", statement, symbol)
            continue
        if isinstance(line_item, str):
            items_by_date[fiscal_date][statement][line_item] = value
    history = [
        LineItems(
            period=period,
            income=items_by_date[period]["income"],
            balance=items_by_date[period]["balance"],
            cash_flow=items_by_date[period]["cash_flow"],
        )
        for period in sorted(items_by_date)
    ]
    logger.info("Loaded %d historical periods for %s", len(history), symbol)
    return FinancialModel(history=history, forecast=[]), filing_dates


def _normalize_exchange_value(value: object) -> object | None:
    """Normalize exchange list values into safe scalar values."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _is_exchange_field_complete(value: object) -> bool:
    """Check whether an exchange list field is present and not 'Unknown'."""
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return False
    return text.upper() != "UNKNOWN"


def _exchange_missing_fields(row: Mapping[str, object]) -> tuple[str, ...]:
    """Return missing exchange fields for completeness checks."""
    return tuple(
        column
        for column in EXCHANGE_LIST_COLUMNS
        if not _is_exchange_field_complete(row.get(column))
    )


def _normalize_share_value(value: object) -> str | None:
    """Normalize share universe values into trimmed strings."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return str(value)


def _normalize_share_code(value: object) -> str | None:
    """Normalize share universe code values to uppercase."""
    text = _normalize_share_value(value)
    if text is None:
        return None
    upper = text.upper()
    return None if upper == "UNKNOWN" else upper


def _iter_earnings_calendar_rows(
    retrieval_date: datetime,
    payload: object | None,
) -> Iterable[dict[str, object]]:
    """Yield upcoming earnings calendar rows from the payload.

    Args:
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw earnings calendar payload.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    if payload is None:
        return []
    return [
        {
            "symbol": code,
            RETRIEVAL_COLUMN: retrieval_date,
            "date": report_date,
            "fiscal_date": fiscal_date,
            "before_after_market": before_after,
            "currency": currency,
            "actual": _to_float(entry.get("actual")),
            "estimate": _to_float(entry.get("estimate")),
            "difference": _to_float(entry.get("difference")),
            "percent": _to_float_allow_percent(entry.get("percent")),
        }
        for entry in _calendar_entries(payload)
        for code in [_calendar_code(entry)]
        if code is not None
        for report_date in [
            _parse_date(
                _first_present(entry, ("report_date", "reportDate", "date"))
            )
        ]
        if report_date is not None
        for fiscal_date in [
            _parse_date(
                _first_present(
                    entry,
                    (
                        "fiscal_date",
                        "fiscalDate",
                        "date",
                        "period_end",
                        "period_end_date",
                        "period",
                    ),
                )
            )
        ]
        for before_after in [
            _normalize_text_value(
                _first_present(
                    entry,
                    ("before_or_after_market", "beforeOrAfterMarket"),
                )
            )
        ]
        for currency in [_normalize_text_value(entry.get("currency"))]
    ]


def _iter_split_calendar_rows(
    retrieval_date: datetime,
    payload: object | None,
) -> Iterable[dict[str, object]]:
    """Yield upcoming splits calendar rows from the payload.

    Args:
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw splits calendar payload.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    if payload is None:
        return []
    return [
        {
            "symbol": code,
            RETRIEVAL_COLUMN: retrieval_date,
            "date": split_date,
            "optionable": _parse_optionable(entry.get("optionable")),
            "old_shares": _to_float(entry.get("old_shares")),
            "new_shares": _to_float(entry.get("new_shares")),
        }
        for entry in _calendar_entries(payload)
        for code in [_calendar_code(entry)]
        if code is not None
        for split_date in [
            _parse_date(
                _first_present(entry, ("split_date", "splitDate", "date"))
            )
        ]
        if split_date is not None
    ]


def _iter_dividend_calendar_rows(
    retrieval_date: datetime,
    payload: object | None,
) -> Iterable[dict[str, object]]:
    """Yield upcoming dividends calendar rows from the payload.

    Args:
        retrieval_date (datetime): When the payload was retrieved.
        payload (object | None): Raw dividends calendar payload.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    if payload is None:
        return []
    return [
        {
            "symbol": code,
            RETRIEVAL_COLUMN: retrieval_date,
            "date": dividend_date,
            "currency": _normalize_text_value(
                _first_present(entry, ("currency", "Currency"))
            ),
            "amount": _to_float(
                _first_present(entry, ("dividend", "amount", "value"))
            ),
            "period": _normalize_text_value(
                _first_present(entry, ("period", "Period"))
            ),
            "declaration_date": _parse_date(
                _first_present(entry, ("declarationDate", "declaration_date"))
            ),
            "record_date": _parse_date(
                _first_present(entry, ("recordDate", "record_date"))
            ),
            "payment_date": _parse_date(
                _first_present(entry, ("paymentDate", "payment_date"))
            ),
        }
        for entry in _calendar_entries(payload)
        for code in [_calendar_code(entry)]
        if code is not None
        for dividend_date in [
            _parse_date(
                _first_present(entry, ("date", "ex_date", "exDate", "dividend_date"))
            )
        ]
        if dividend_date is not None
    ]


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
    return [
        {
            "symbol": symbol,
            "date": entry_date,
            "name": entry.get("name", "").strip(),
            "category": category,
            "retrieval_date": retrieval_date,
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
        for entry_date in [_parse_date(entry.get("date"))]
        if entry_date is not None
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
    row_builder = partial(_insider_row, symbol, retrieval_date)
    return [row for row in map(row_builder, transactions.values()) if row is not None]


def _insider_row(
    symbol: str,
    retrieval_date: datetime,
    entry: object,
) -> dict[str, object] | None:
    """Build a row for a single insider transaction entry.

    Args:
        symbol (str): Ticker symbol for the payload.
        retrieval_date (datetime): Retrieval timestamp.
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
    parsed_date = _parse_date(date_str)
    if parsed_date is None:
        return None
    transaction_date = _parse_date(entry.get("transactionDate"))
    return {
        "symbol": symbol,
        "date": parsed_date,
        "ownerName": owner.strip(),
        "retrieval_date": retrieval_date,
        "transactionDate": transaction_date,
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


def _market_metrics_row(
    symbol: str,
    retrieval_date: datetime,
    raw_data: Mapping[str, object],
) -> dict[str, object] | None:
    """Build a wide market metrics row from supported payload sections."""
    entries = _market_metric_entries(raw_data)
    if not entries:
        logger.debug("Market metrics payload contained no usable entries")
        return None
    metrics: dict[str, tuple[object, str]] = {}
    collisions: list[dict[str, str]] = []
    for metric, raw_value, section in entries:
        if metric in metrics:
            existing_value, existing_section = metrics[metric]
            if existing_value is not None and raw_value is not None:
                collisions.append(
                    {
                        "metric": metric,
                        "kept_section": existing_section,
                        "dropped_section": section,
                    }
                )
                continue
            if raw_value is None:
                continue
        metrics[metric] = (raw_value, section)
    if collisions:
        logger.debug(
            "Market metrics collisions (sample): %s",
            collisions[:25],
        )
    unknown_metrics = [
        metric for metric in metrics.keys() if metric not in MARKET_METRIC_TYPES
    ]
    if unknown_metrics:
        logger.debug(
            "Market metrics columns missing from schema (sample): %s",
            sorted(unknown_metrics)[:25],
        )
    row = {
        "symbol": symbol,
        "retrieval_date": retrieval_date,
        **{metric: None for metric in MARKET_METRIC_COLUMNS},
    }
    for metric, (raw_value, _) in metrics.items():
        if metric not in MARKET_METRIC_TYPES:
            continue
        row[metric] = _market_metric_value(metric, raw_value)
    return row


def _market_metric_entries(raw_data: Mapping[str, object]) -> list[tuple[str, object, str]]:
    """Collect market metrics entries from supported payload sections."""
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

    def section_entries(section: str) -> Iterable[tuple[str, object, str]]:
        """Build metric entries for a section."""
        data = raw_data.get(section)
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
        return [
            (metric, raw_value, section)
            for metric, raw_value in metrics.items()
        ]

    return list(mapcat(section_entries, sections))


def _market_metric_value(metric: str, raw_value: object) -> object | None:
    """Coerce a market metric value to the schema-defined type."""
    metric_type = MARKET_METRIC_TYPES.get(metric, "text")
    if metric_type == "float":
        return _to_float(raw_value)
    if metric_type == "date":
        return _parse_date(raw_value)
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        return stripped if stripped else None
    return str(raw_value)


def _metric_param_name(column: str) -> str:
    """Build a safe parameter name for SQL binds."""
    safe = "".join(char if char.isalnum() else "_" for char in column)
    if not safe or safe[0].isdigit():
        safe = f"m_{safe}"
    return f"p_{safe}"


def _quote_identifier(identifier: str) -> str:
    """Quote an SQL identifier."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _iter_price_rows(
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    raw_data: object,
) -> Iterable[dict[str, object]]:
    """Yield price rows from an EODHD end-of-day payload.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): When the payload was retrieved.
        raw_data (object): Raw provider payload for prices.

    Returns:
        Iterable[dict[str, object]]: Row dictionaries for insertion.
    """
    base = {
        "symbol": symbol,
        "retrieval_date": retrieval_date,
        "provider": provider,
    }
    return [
        {
            **base,
            "date": price_date,
            **{
                field: _first_value(entry, keys)
                for field, keys in PRICE_FIELD_MAP.items()
            },
        }
        for entry in _price_entries(raw_data)
        if isinstance(entry, Mapping)
        for price_date in [_parse_date(entry.get("date"))]
        if price_date is not None
    ]


def _price_entries(raw_data: object) -> Iterable[Mapping[str, object]]:
    """Normalize price payloads into an iterable of entry mappings.

    Args:
        raw_data (object): Raw price payload.

    Returns:
        Iterable[Mapping[str, object]]: Iterable of entry mappings.
    """
    if isinstance(raw_data, list):
        return raw_data
    if isinstance(raw_data, Mapping):
        return raw_data.values()
    return []


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
        engine (Engine): SQLAlchemy engine for Postgres.
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
    all_items = [*model.history, *model.forecast]
    items_with_flags = (
        (item, index >= history_len) for index, item in enumerate(all_items)
    )
    return (
        {
            "symbol": symbol,
            "fiscal_date": item.period,
            "filing_date": filing_dates.get(item.period, item.period),
            "retrieval_date": retrieval_date,
            "period_type": period_type,
            "statement": statement,
            "line_item": line_item,
            "value_source": value_source,
            "value": value,
            "is_forecast": is_forecast,
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
        engine (Engine): SQLAlchemy engine for Postgres.
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
            retrieval_date=retrieval_date,
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
        retrieval_date=retrieval_date,
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
    retrieval_date: datetime,
    period_label: str,
    statement: str,
    period_block: Mapping[str, object] | None,
    field_map: Mapping[str, tuple[str, ...]],
) -> Iterable[dict[str, object]]:
    """Yield reported rows for a single statement and period block.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): Retrieval timestamp.
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
            retrieval_date=retrieval_date,
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
    retrieval_date: datetime,
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
        retrieval_date (datetime): Retrieval timestamp.
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
        "fiscal_date": fiscal_date,
        "filing_date": filing_date,
        "retrieval_date": retrieval_date,
        "period_type": period_label,
        "statement": statement,
        "is_forecast": False,
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
    retrieval_date: datetime,
    outstanding: object,
) -> list[dict[str, object]]:
    """Yield reported rows for outstanding shares.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): Retrieval timestamp.
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
            retrieval_date=retrieval_date,
            period_label=label,
            block=outstanding.get(period_key),
        )

    return list(mapcat(period_rows, period_types))


def _outstanding_period_rows(
    symbol: str,
    provider: str,
    retrieval_date: datetime,
    period_label: str,
    block: object,
) -> list[dict[str, object]]:
    """Yield outstanding shares rows for a specific period type.

    Args:
        symbol (str): Ticker symbol for the payload.
        provider (str): Provider name (e.g., "EODHD").
        retrieval_date (datetime): Retrieval timestamp.
        period_label (str): Period type label ("annual" or "quarterly").
        block (object): Outstanding shares block (mapping or list).

    Returns:
        list[dict[str, object]]: Row dictionaries for insertion.
    """
    entries = _normalize_outstanding_block(block)
    rows = [
        {
            "symbol": symbol,
            "fiscal_date": fiscal_date,
            "filing_date": fiscal_date,
            "retrieval_date": retrieval_date,
            "period_type": period_label,
            "statement": "multi_statement",
            "line_item": "shares",
            "value_source": "reported",
            "value": shares,
            "is_forecast": False,
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
    conn: Connection,
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


def _first_present(values: Mapping[str, object], keys: tuple[str, ...]) -> object | None:
    """Return the first non-empty value from a mapping by key preference.

    Args:
        values (Mapping[str, object]): Mapping of raw fields.
        keys (tuple[str, ...]): Candidate keys in order.

    Returns:
        object | None: First non-empty value, if present.
    """
    for key in keys:
        if key in values:
            value = values.get(key)
            if value is not None:
                return value
    return None


def _normalize_text_value(value: object) -> str | None:
    """Normalize a value into a trimmed string when possible."""
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return None


def _calendar_entries(payload: object) -> Iterable[Mapping[str, object]]:
    """Normalize calendar payloads into an iterable of entry mappings."""
    if isinstance(payload, list):
        entries = [entry for entry in payload if isinstance(entry, Mapping)]
    elif isinstance(payload, Mapping):
        data = payload.get("data")
        earnings = payload.get("earnings")
        splits = payload.get("splits")
        dividends = payload.get("dividends")
        if isinstance(data, list):
            entries = [entry for entry in data if isinstance(entry, Mapping)]
        elif isinstance(earnings, list):
            entries = [entry for entry in earnings if isinstance(entry, Mapping)]
        elif isinstance(splits, list):
            entries = [entry for entry in splits if isinstance(entry, Mapping)]
        elif isinstance(dividends, list):
            entries = [entry for entry in dividends if isinstance(entry, Mapping)]
        else:
            entries = [entry for entry in payload.values() if isinstance(entry, Mapping)]
    else:
        entries = []
    return [entry for entry in entries if _calendar_code(entry) is not None]


def _calendar_code(entry: Mapping[str, object]) -> str | None:
    """Extract a ticker code from a calendar entry."""
    value = _first_present(entry, ("code", "Code", "symbol", "ticker"))
    return _normalize_text_value(value)


def _parse_optionable(value: object) -> bool | None:
    """Parse the splits optionable flag from a calendar entry."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized == "Y":
            return True
        if normalized == "N":
            return False
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


def _to_float_allow_percent(value: object) -> float | None:
    """Convert a provider value to float, allowing trailing percent signs."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.endswith("%"):
            stripped = stripped[:-1].strip()
        return _to_float(stripped)
    return _to_float(value)


def _parse_date(value: object) -> date | None:
    """Parse a date from ISO string values.

    Args:
        value (object): Raw date value.

    Returns:
        date | None: Parsed date if possible.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return date.fromisoformat(stripped)
        except ValueError:
            normalized = stripped[:-1] + "+00:00" if stripped.endswith("Z") else stripped
            try:
                return datetime.fromisoformat(normalized).date()
            except ValueError:
                return None
    return None
