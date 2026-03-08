--------------------------------------------------------------------------------
-- dim_account: Account Dimension, Staging Table & Population Procedure
-- ===============================================================
-- This file contains:
--   1. Staging table : stg_accounts       (mirrors CSV from data_generator.py)
--   2. Seed data     : dim_account_type   (CHK / SAV / TD / INV / CRD)
--   3. Dimension table: dim_account       (SCD Type 1 — overwrite on change)
--   4. Procedure     : sp_populate_dim_account
--
-- Designed for: Spark-ling Banking Simulation (PostgreSQL / AWS RDS)
-- Data source  : src/data_generator.py → data/raw/accounts.csv
--
-- LEARNING NOTES:
-- ---------------
-- dim_account uses SCD Type 1 (overwrite) because:
--   - Account balance and status change frequently
--   - Full history is tracked at transaction level (fact_transaction)
--   - Keeping a Type 2 history here would inflate the table unnecessarily
--
-- Contrast with dim_customer which uses SCD Type 2 because:
--   - Customer segment / address changes are analytically significant
--   - We need to know "what segment was this customer WHEN the transaction happened"
--------------------------------------------------------------------------------

-- ============================================================================
-- STAGING TABLE: stg_accounts
-- ============================================================================
-- Mirrors the exact CSV schema produced by data_generator.py.
-- This is the raw landing zone before type-casting and code mapping.
-- Pipeline pattern: TRUNCATE → COPY from S3 → CALL sp_populate_dim_account()
CREATE TABLE IF NOT EXISTS stg_accounts (
    account_id          VARCHAR(20),
    customer_id         VARCHAR(20),
    branch_id           VARCHAR(10),
    account_type        VARCHAR(30),        -- Raw text: "Checking", "Savings", etc.
    balance             NUMERIC(18,2),
    currency            VARCHAR(3),
    status              VARCHAR(20),        -- Raw text: "Active", "Dormant", etc.
    opened_date         VARCHAR(10),        -- Raw string YYYY-MM-DD from CSV
    last_activity_date  VARCHAR(10),        -- Raw string YYYY-MM-DD from CSV
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_account_id ON stg_accounts(account_id);


-- ============================================================================
-- SEED DATA: dim_account_type
-- ============================================================================
-- Maps the 5 account_type text values from data_generator.py to
-- standardized short codes used as FK across the warehouse.
--
-- Interest rates aligned with Vietnamese banking benchmarks (2024-2025):
--   Savings:     ~4-5% p.a. (Big-4 average)
--   Term Deposit: ~6-7% p.a. (12-month tenor)
--   Credit:      ~18% p.a.  (consumer revolving credit)
--
-- Vietnamese banks referenced: Vietcombank, BIDV, Techcombank, MB Bank
INSERT INTO dim_account_type (account_type_code, account_type_name, category, interest_rate, min_balance)
VALUES
    ('CHK', 'Checking Account',   'Deposit',     0.00,        0),   -- Thanh toan
    ('SAV', 'Savings Account',    'Deposit',      4.50,   50000),   -- Tiet kiem khong ky han
    ('TD',  'Term Deposit',       'Deposit',      6.80, 1000000),   -- Tiet kiem co ky han
    ('INV', 'Investment Account', 'Investment',   0.00, 5000000),   -- Dau tu / chung khoan
    ('CRD', 'Credit Account',     'Credit',      18.00,        0)   -- Tin dung / the tin dung
ON CONFLICT (account_type_code) DO UPDATE SET
    account_type_name = EXCLUDED.account_type_name,
    interest_rate     = EXCLUDED.interest_rate,
    min_balance       = EXCLUDED.min_balance,
    last_modified     = CURRENT_TIMESTAMP;


-- ============================================================================
-- DIMENSION TABLE: dim_account
-- ============================================================================
-- Grain        : One row per account (business key = account_id)
-- SCD Strategy : Type 1 — mutable fields (balance, status) are overwritten
-- Key joins    :
--   customer_id       → dim_customer.customer_id  (business key, no FK due to SCD Type 2)
--   branch_id         → dim_branch.branch_id       (FK on unique business key)
--   account_type_code → dim_account_type.account_type_code (FK on unique code)
--
-- Balance tiering (used downstream in reporting):
--   Micro    : < 1,000,000 VND
--   Retail   : 1,000,000 – 100,000,000 VND
--   Affluent : 100,000,000 – 1,000,000,000 VND
--   Private  : > 1,000,000,000 VND
CREATE TABLE IF NOT EXISTS dim_account (
    account_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id          VARCHAR(20)   NOT NULL UNIQUE,   -- Business key: ACCT000001
    customer_id         VARCHAR(20)   NOT NULL,          -- Logical FK to dim_customer
    branch_id           VARCHAR(10)   NOT NULL,          -- FK to dim_branch.branch_id
    account_type_code   VARCHAR(10)   NOT NULL,          -- FK to dim_account_type.account_type_code
    currency            VARCHAR(3)    DEFAULT 'VND',
    initial_balance     NUMERIC(18,2) DEFAULT 0,         -- Balance captured on first load
    current_balance     NUMERIC(18,2) DEFAULT 0,         -- Overwritten each pipeline run (SCD1)
    status              VARCHAR(20)   DEFAULT 'Active',  -- Active | Dormant | Closed | Frozen
    opened_date         DATE          NOT NULL,
    closed_date         DATE,                            -- NULL = account still open
    last_activity_date  DATE,
    last_modified       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    -- Formal FK constraints where referencing a UNIQUE business key is safe
    CONSTRAINT fk_account_branch FOREIGN KEY (branch_id)
        REFERENCES dim_branch(branch_id),
    CONSTRAINT fk_account_type   FOREIGN KEY (account_type_code)
        REFERENCES dim_account_type(account_type_code)
);

CREATE INDEX IF NOT EXISTS idx_account_customer ON dim_account(customer_id);
CREATE INDEX IF NOT EXISTS idx_account_branch   ON dim_account(branch_id);
CREATE INDEX IF NOT EXISTS idx_account_type     ON dim_account(account_type_code);
CREATE INDEX IF NOT EXISTS idx_account_status   ON dim_account(status);
CREATE INDEX IF NOT EXISTS idx_account_cdc      ON dim_account(last_modified);

-- Register in CDC watermark so the pipeline tracks this table
INSERT INTO cdc_watermark (table_name, last_watermark)
VALUES ('dim_account', '2020-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;


-- ============================================================================
-- PROCEDURE: sp_populate_dim_account
-- ============================================================================
-- Purpose  : ETL from stg_accounts → dim_account
-- Strategy : UPSERT (INSERT … ON CONFLICT UPDATE) — SCD Type 1
-- Trigger  : Called after stg_accounts is loaded from accounts.csv
--
-- Key transformations:
--   1. Maps free-text account_type → standardized code (CHK/SAV/TD/INV/CRD)
--   2. Casts VARCHAR date strings → DATE using TO_DATE()
--   3. Derives closed_date = last_activity_date only when status = 'Closed'
--   4. Guards referential integrity: skips rows whose customer or branch
--      does not exist in the dimension tables yet
--   5. Preserves initial_balance on first load; never overwrites it on upsert
--   6. Updates cdc_watermark on completion
--
-- Usage:
--   CALL sp_populate_dim_account();
CREATE OR REPLACE PROCEDURE sp_populate_dim_account()
LANGUAGE plpgsql
AS $$
DECLARE
    v_affected INTEGER := 0;
BEGIN
    RAISE NOTICE '[sp_populate_dim_account] Starting at %', CURRENT_TIMESTAMP;

    INSERT INTO dim_account (
        account_id,
        customer_id,
        branch_id,
        account_type_code,
        currency,
        initial_balance,
        current_balance,
        status,
        opened_date,
        closed_date,
        last_activity_date,
        last_modified
    )
    SELECT
        s.account_id,
        s.customer_id,
        s.branch_id,
        -- Map raw account_type text to warehouse standard code
        CASE s.account_type
            WHEN 'Checking'     THEN 'CHK'
            WHEN 'Savings'      THEN 'SAV'
            WHEN 'Term Deposit' THEN 'TD'
            WHEN 'Investment'   THEN 'INV'
            WHEN 'Credit'       THEN 'CRD'
            ELSE                     'OTH'  -- Safety net for unexpected values
        END                                         AS account_type_code,
        COALESCE(s.currency, 'VND')                 AS currency,
        s.balance                                   AS initial_balance,
        s.balance                                   AS current_balance,
        s.status,
        TO_DATE(s.opened_date,        'YYYY-MM-DD') AS opened_date,
        -- closed_date is only meaningful when the account is explicitly Closed
        CASE s.status
            WHEN 'Closed' THEN TO_DATE(s.last_activity_date, 'YYYY-MM-DD')
            ELSE NULL
        END                                         AS closed_date,
        TO_DATE(s.last_activity_date, 'YYYY-MM-DD') AS last_activity_date,
        CURRENT_TIMESTAMP
    FROM stg_accounts s
    -- Referential integrity guard: only load if related dimension rows exist
    WHERE EXISTS (
        SELECT 1 FROM dim_customer dc
        WHERE  dc.customer_id = s.customer_id
          AND  dc.is_current  = TRUE
    )
    AND EXISTS (
        SELECT 1 FROM dim_branch db
        WHERE  db.branch_id = s.branch_id
    )
    ON CONFLICT (account_id) DO UPDATE SET
        -- SCD Type 1: only mutable operational fields are overwritten
        current_balance    = EXCLUDED.current_balance,
        status             = EXCLUDED.status,
        closed_date        = EXCLUDED.closed_date,
        last_activity_date = EXCLUDED.last_activity_date,
        last_modified      = CURRENT_TIMESTAMP;
        -- NOTE: initial_balance and opened_date are intentionally NOT updated

    GET DIAGNOSTICS v_affected = ROW_COUNT;

    -- Sync CDC watermark
    UPDATE cdc_watermark SET
        last_watermark  = CURRENT_TIMESTAMP,
        last_row_count  = v_affected,
        last_run_status = 'SUCCESS',
        updated_at      = CURRENT_TIMESTAMP
    WHERE table_name = 'dim_account';

    RAISE NOTICE '[sp_populate_dim_account] Completed — % rows affected', v_affected;

EXCEPTION WHEN OTHERS THEN
    UPDATE cdc_watermark SET
        last_run_status = 'FAILED',
        updated_at      = CURRENT_TIMESTAMP
    WHERE table_name = 'dim_account';
    RAISE EXCEPTION '[sp_populate_dim_account] Failed: %', SQLERRM;
END;
$$;
