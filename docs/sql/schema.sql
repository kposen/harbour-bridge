-- SQLite schema for financial facts.
CREATE TABLE financial_facts (
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

CREATE INDEX IX_financial_facts_symbol_fiscal
    ON financial_facts (symbol, fiscal_date, period_type);

CREATE INDEX IX_financial_facts_retrieval
    ON financial_facts (retrieval_date);

CREATE TABLE market_metrics (
    symbol TEXT NOT NULL,
    retrieval_date TEXT NOT NULL,
    section TEXT NOT NULL,
    metric TEXT NOT NULL,
    value_float REAL NULL,
    value_text TEXT NULL,
    value_type TEXT NOT NULL,
    PRIMARY KEY (symbol, retrieval_date, section, metric)
);

CREATE INDEX IX_market_metrics_symbol
    ON market_metrics (symbol, retrieval_date);

CREATE TABLE earnings (
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

CREATE INDEX IX_earnings_symbol_date
    ON earnings (symbol, date);

CREATE TABLE holders (
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

CREATE INDEX IX_holders_symbol_date
    ON holders (symbol, date);

CREATE TABLE insider_transactions (
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

CREATE INDEX IX_insider_transactions_symbol_date
    ON insider_transactions (symbol, date);

CREATE TABLE listings (
    code TEXT NOT NULL,
    exchange TEXT NOT NULL,
    retrieval_date TEXT NOT NULL,
    primary_ticker TEXT NOT NULL,
    name TEXT NULL,
    PRIMARY KEY (code, exchange, retrieval_date)
);

CREATE INDEX IX_listings_primary_ticker
    ON listings (primary_ticker, retrieval_date);

CREATE TABLE prices (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    retrieval_date TEXT NOT NULL,
    provider TEXT NOT NULL,
    open REAL NULL,
    high REAL NULL,
    low REAL NULL,
    close REAL NULL,
    adjusted_close REAL NULL,
    volume REAL NULL,
    PRIMARY KEY (symbol, date, retrieval_date, provider)
);

CREATE INDEX IX_prices_symbol_date
    ON prices (symbol, date);
