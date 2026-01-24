-- Postgres schema for financial facts.
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
    CONSTRAINT PK_financial_facts PRIMARY KEY (
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
    section TEXT NOT NULL,
    metric TEXT NOT NULL,
    value_float DOUBLE PRECISION NULL,
    value_text TEXT NULL,
    value_type TEXT NOT NULL,
    PRIMARY KEY (symbol, retrieval_date, section, metric)
);

CREATE INDEX IF NOT EXISTS IX_market_metrics_symbol
    ON market_metrics (symbol, retrieval_date);

CREATE TABLE IF NOT EXISTS earnings (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    period_type TEXT NOT NULL,
    field TEXT NOT NULL,
    retrieval_date TIMESTAMPTZ NOT NULL,
    value_float DOUBLE PRECISION NULL,
    value_text TEXT NULL,
    value_type TEXT NOT NULL,
    PRIMARY KEY (symbol, date, period_type, field, retrieval_date)
);

CREATE INDEX IF NOT EXISTS IX_earnings_symbol_date
    ON earnings (symbol, date);

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

CREATE TABLE IF NOT EXISTS listings (
    code TEXT NOT NULL,
    exchange TEXT NOT NULL,
    retrieval_date TIMESTAMPTZ NOT NULL,
    primary_ticker TEXT NOT NULL,
    name TEXT NULL,
    PRIMARY KEY (code, exchange, retrieval_date)
);

CREATE INDEX IF NOT EXISTS IX_listings_primary_ticker
    ON listings (primary_ticker, retrieval_date);

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

CREATE TABLE IF NOT EXISTS exchange_list (
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

CREATE INDEX IF NOT EXISTS IX_exchange_list_code
    ON exchange_list (code);

CREATE TABLE IF NOT EXISTS share_universe (
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

CREATE INDEX IF NOT EXISTS IX_share_universe_symbol
    ON share_universe (symbol, exchange);

CREATE TABLE IF NOT EXISTS corporate_actions_calendar (
    symbol TEXT NOT NULL,
    date_retrieved TIMESTAMPTZ NOT NULL,
    earnings_report_date DATE NULL,
    earnings_fiscal_date DATE NULL,
    earnings_before_after_market TEXT NULL,
    earnings_currency TEXT NULL,
    earnings_actual DOUBLE PRECISION NULL,
    earnings_estimate DOUBLE PRECISION NULL,
    earnings_difference DOUBLE PRECISION NULL,
    earnings_percent DOUBLE PRECISION NULL,
    dividend_date DATE NULL,
    dividend_currency TEXT NULL,
    dividend_amount DOUBLE PRECISION NULL,
    dividend_period TEXT NULL,
    dividend_declaration_date DATE NULL,
    dividend_record_date DATE NULL,
    dividend_payment_date DATE NULL,
    split_date DATE NULL,
    split_optionable BOOLEAN NULL,
    split_old_shares DOUBLE PRECISION NULL,
    split_new_shares DOUBLE PRECISION NULL
);

CREATE INDEX IF NOT EXISTS IX_corporate_actions_symbol_earnings
    ON corporate_actions_calendar (symbol, earnings_report_date);

CREATE INDEX IF NOT EXISTS IX_corporate_actions_symbol_split
    ON corporate_actions_calendar (symbol, split_date);
