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
