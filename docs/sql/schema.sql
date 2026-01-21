-- SQL Server schema for financial facts.
CREATE TABLE dbo.financial_facts (
    symbol NVARCHAR(32) NOT NULL,
    fiscal_date DATE NOT NULL,
    filing_date DATE NOT NULL,
    retrieval_date DATETIME2 NOT NULL,
    period_type NVARCHAR(16) NOT NULL,
    statement NVARCHAR(16) NOT NULL,
    line_item NVARCHAR(64) NOT NULL,
    value_source NVARCHAR(16) NOT NULL,
    value FLOAT NULL,
    is_forecast BIT NOT NULL,
    provider NVARCHAR(32) NOT NULL,
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
    ON dbo.financial_facts (symbol, fiscal_date, period_type);

CREATE INDEX IX_financial_facts_retrieval
    ON dbo.financial_facts (retrieval_date);
