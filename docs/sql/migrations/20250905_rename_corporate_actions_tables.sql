-- Rename corporate_actions_* tables to earnings/dividends/splits.
-- Drops legacy earnings table when it matches the old fundamentals schema.

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

    IF to_regclass('public.corporate_actions_earnings') IS NOT NULL THEN
        IF to_regclass('public.earnings') IS NULL THEN
            ALTER TABLE corporate_actions_earnings RENAME TO earnings;
        ELSE
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
            SELECT
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
            FROM corporate_actions_earnings
            ON CONFLICT DO NOTHING;
            DROP TABLE corporate_actions_earnings;
        END IF;
    END IF;

    IF to_regclass('public.corporate_actions_dividends') IS NOT NULL THEN
        IF to_regclass('public.dividends') IS NULL THEN
            ALTER TABLE corporate_actions_dividends RENAME TO dividends;
        ELSE
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
            SELECT
                symbol,
                retrieval_date,
                date,
                currency,
                amount,
                period,
                declaration_date,
                record_date,
                payment_date
            FROM corporate_actions_dividends
            ON CONFLICT DO NOTHING;
            DROP TABLE corporate_actions_dividends;
        END IF;
    END IF;

    IF to_regclass('public.corporate_actions_splits') IS NOT NULL THEN
        IF to_regclass('public.splits') IS NULL THEN
            ALTER TABLE corporate_actions_splits RENAME TO splits;
        ELSE
            INSERT INTO splits (
                symbol,
                retrieval_date,
                date,
                optionable,
                old_shares,
                new_shares
            )
            SELECT
                symbol,
                retrieval_date,
                date,
                optionable,
                old_shares,
                new_shares
            FROM corporate_actions_splits
            ON CONFLICT DO NOTHING;
            DROP TABLE corporate_actions_splits;
        END IF;
    END IF;
END $$;
