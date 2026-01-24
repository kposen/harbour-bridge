-- Split corporate_actions_calendar into earnings/dividends/splits tables.
-- Safe to run multiple times; data inserts use ON CONFLICT DO NOTHING.

DO $$
BEGIN
    IF to_regclass('public.earnings') IS NOT NULL AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'earnings'
          AND column_name = 'period_type'
    ) THEN
        DROP TABLE earnings;
    END IF;
END $$;

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

DO $$
DECLARE
    retrieval_col TEXT;
BEGIN
    IF to_regclass('public.corporate_actions_calendar') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'corporate_actions_calendar'
              AND column_name = 'retrieval_date'
        ) THEN
            retrieval_col := 'retrieval_date';
        ELSIF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'corporate_actions_calendar'
              AND column_name = 'date_retrieved'
        ) THEN
            retrieval_col := 'date_retrieved';
        ELSE
            RAISE EXCEPTION 'corporate_actions_calendar missing retrieval_date/date_retrieved';
        END IF;

        EXECUTE format(
            'INSERT INTO earnings (
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
            SELECT
                symbol,
                %I,
                earnings_report_date,
                earnings_fiscal_date,
                earnings_before_after_market,
                earnings_currency,
                earnings_actual,
                earnings_estimate,
                earnings_difference,
                earnings_percent
            FROM corporate_actions_calendar
            WHERE earnings_report_date IS NOT NULL
            ON CONFLICT DO NOTHING',
            retrieval_col
        );

        EXECUTE format(
            'INSERT INTO dividends (
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
            SELECT
                symbol,
                %I,
                dividend_date,
                dividend_currency,
                dividend_amount,
                dividend_period,
                dividend_declaration_date,
                dividend_record_date,
                dividend_payment_date
            FROM corporate_actions_calendar
            WHERE dividend_date IS NOT NULL
            ON CONFLICT DO NOTHING',
            retrieval_col
        );

        EXECUTE format(
            'INSERT INTO splits (
                symbol,
                retrieval_date,
                date,
                optionable,
                old_shares,
                new_shares
            )
            SELECT
                symbol,
                %I,
                split_date,
                split_optionable,
                split_old_shares,
                split_new_shares
            FROM corporate_actions_calendar
            WHERE split_date IS NOT NULL
            ON CONFLICT DO NOTHING',
            retrieval_col
        );

        DROP TABLE corporate_actions_calendar;
    END IF;
END $$;
