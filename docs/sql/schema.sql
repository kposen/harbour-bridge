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
    "200DayMA" DOUBLE PRECISION NULL,
    "50DayMA" DOUBLE PRECISION NULL,
    "52WeekHigh" DOUBLE PRECISION NULL,
    "52WeekLow" DOUBLE PRECISION NULL,
    "Address" TEXT NULL,
    "Beta" DOUBLE PRECISION NULL,
    "BookValue" DOUBLE PRECISION NULL,
    "Buy" DOUBLE PRECISION NULL,
    "CIK" TEXT NULL,
    "CUSIP" TEXT NULL,
    "City" TEXT NULL,
    "Code" TEXT NULL,
    "Country" TEXT NULL,
    "CountryISO" TEXT NULL,
    "CountryName" TEXT NULL,
    "CurrencyCode" TEXT NULL,
    "CurrencyName" TEXT NULL,
    "CurrencySymbol" TEXT NULL,
    "Description" TEXT NULL,
    "DilutedEpsTTM" DOUBLE PRECISION NULL,
    "DividendDate" DATE NULL,
    "DividendShare" DOUBLE PRECISION NULL,
    "DividendYield" DOUBLE PRECISION NULL,
    "EBITDA" DOUBLE PRECISION NULL,
    "EPSEstimateCurrentQuarter" DOUBLE PRECISION NULL,
    "EPSEstimateCurrentYear" DOUBLE PRECISION NULL,
    "EPSEstimateNextQuarter" DOUBLE PRECISION NULL,
    "EPSEstimateNextYear" DOUBLE PRECISION NULL,
    "EarningsShare" DOUBLE PRECISION NULL,
    "EmployerIdNumber" TEXT NULL,
    "EnterpriseValue" DOUBLE PRECISION NULL,
    "EnterpriseValueEbitda" DOUBLE PRECISION NULL,
    "EnterpriseValueRevenue" DOUBLE PRECISION NULL,
    "ExDividendDate" DATE NULL,
    "Exchange" TEXT NULL,
    "FiscalYearEnd" TEXT NULL,
    "ForwardAnnualDividendRate" DOUBLE PRECISION NULL,
    "ForwardAnnualDividendYield" DOUBLE PRECISION NULL,
    "ForwardPE" DOUBLE PRECISION NULL,
    "FullTimeEmployees" DOUBLE PRECISION NULL,
    "GicGroup" TEXT NULL,
    "GicIndustry" TEXT NULL,
    "GicSector" TEXT NULL,
    "GicSubIndustry" TEXT NULL,
    "GrossProfitTTM" DOUBLE PRECISION NULL,
    "Hold" DOUBLE PRECISION NULL,
    "HomeCategory" TEXT NULL,
    "IPODate" DATE NULL,
    "ISIN" TEXT NULL,
    "Industry" TEXT NULL,
    "InternationalDomestic" TEXT NULL,
    "IsDelisted" DOUBLE PRECISION NULL,
    "LEI" TEXT NULL,
    "LastSplitDate" DATE NULL,
    "LastSplitFactor" TEXT NULL,
    "LogoURL" TEXT NULL,
    "MarketCapitalization" DOUBLE PRECISION NULL,
    "MarketCapitalizationMln" DOUBLE PRECISION NULL,
    "MostRecentQuarter" DATE NULL,
    "Name" TEXT NULL,
    "OpenFigi" TEXT NULL,
    "OperatingMarginTTM" DOUBLE PRECISION NULL,
    "PEGRatio" DOUBLE PRECISION NULL,
    "PERatio" DOUBLE PRECISION NULL,
    "PayoutRatio" DOUBLE PRECISION NULL,
    "PercentInsiders" DOUBLE PRECISION NULL,
    "PercentInstitutions" DOUBLE PRECISION NULL,
    "Phone" TEXT NULL,
    "PriceBookMRQ" DOUBLE PRECISION NULL,
    "PriceSalesTTM" DOUBLE PRECISION NULL,
    "PrimaryTicker" TEXT NULL,
    "ProfitMargin" DOUBLE PRECISION NULL,
    "QuarterlyEarningsGrowthYOY" DOUBLE PRECISION NULL,
    "QuarterlyRevenueGrowthYOY" DOUBLE PRECISION NULL,
    "Rating" DOUBLE PRECISION NULL,
    "ReturnOnAssetsTTM" DOUBLE PRECISION NULL,
    "ReturnOnEquityTTM" DOUBLE PRECISION NULL,
    "RevenuePerShareTTM" DOUBLE PRECISION NULL,
    "RevenueTTM" DOUBLE PRECISION NULL,
    "Sector" TEXT NULL,
    "Sell" DOUBLE PRECISION NULL,
    "SharesFloat" DOUBLE PRECISION NULL,
    "SharesOutstanding" DOUBLE PRECISION NULL,
    "SharesShort" DOUBLE PRECISION NULL,
    "SharesShortPriorMonth" DOUBLE PRECISION NULL,
    "ShortPercent" DOUBLE PRECISION NULL,
    "ShortPercentFloat" DOUBLE PRECISION NULL,
    "ShortRatio" DOUBLE PRECISION NULL,
    "State" TEXT NULL,
    "Street" TEXT NULL,
    "StrongBuy" DOUBLE PRECISION NULL,
    "StrongSell" DOUBLE PRECISION NULL,
    "TargetPrice" DOUBLE PRECISION NULL,
    "TrailingPE" DOUBLE PRECISION NULL,
    "Type" TEXT NULL,
    "UpdatedAt" DATE NULL,
    "WallStreetTargetPrice" DOUBLE PRECISION NULL,
    "WebURL" TEXT NULL,
    "ZIP" TEXT NULL,
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
