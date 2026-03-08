--------------------------------------------------------------------------------
-- PostgreSQL Schema for Spark-ling Banking Migration Simulation
-- ===============================================================
-- Creates dimension and fact tables for a banking data warehouse.
--
-- LEARNING NOTES:
-- ---------------
-- PostgreSQL vs Oracle syntax differences:
--   Oracle VARCHAR2(n)    → PostgreSQL VARCHAR(n)
--   Oracle NUMBER(p,s)    → PostgreSQL NUMERIC(p,s)
--   Oracle NUMBER         → PostgreSQL BIGINT or SERIAL
--   Oracle DATE           → PostgreSQL DATE (date only, no time)
--   Oracle TIMESTAMP      → PostgreSQL TIMESTAMP
--   Oracle SYSTIMESTAMP   → PostgreSQL CURRENT_TIMESTAMP
--   Oracle IDENTITY       → PostgreSQL GENERATED ALWAYS AS IDENTITY
--   Oracle NVL()          → PostgreSQL COALESCE()
--   Oracle SYSDATE        → PostgreSQL CURRENT_DATE
--
-- These tables simulate what you'd find in a bank's OLTP system
-- (like Techcombank's core banking system).
--
-- USAGE:
--   psql -h <endpoint> -U admin -d sparkdb -f aws/rds/rds_schema.sql
--   OR run from rds_seed_data.py (which creates tables if they don't exist)
--------------------------------------------------------------------------------

-- ============================================================================
-- DIM_DATE: Calendar Dimension
-- ============================================================================
-- LEARNING: Every data warehouse needs a date dimension. It pre-computes
-- fiscal periods, holidays, quarter info, etc. so downstream queries
-- don't have to recalculate these repeatedly.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,              -- YYYYMMDD format
    full_date       DATE NOT NULL UNIQUE,
    day_of_week     INTEGER NOT NULL,                 -- 1=Monday, 7=Sunday
    day_name        VARCHAR(10) NOT NULL,             -- Monday, Tuesday, etc.
    day_of_month    INTEGER NOT NULL,
    day_of_year     INTEGER NOT NULL,
    week_of_year    INTEGER NOT NULL,
    month_number    INTEGER NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    is_weekend      BOOLEAN DEFAULT FALSE,
    is_holiday      BOOLEAN DEFAULT FALSE,
    fiscal_quarter  INTEGER NOT NULL,                 -- Vietnamese fiscal year = calendar year
    fiscal_year     INTEGER NOT NULL,
    last_modified   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);


-- ============================================================================
-- DIM_BRANCH: Bank Branch Dimension
-- ============================================================================
-- LEARNING: Branch dimension captures the organizational hierarchy.
-- In Vietnamese banking, branches map to: Region → City → Branch.
CREATE TABLE IF NOT EXISTS dim_branch (
    branch_key      SERIAL PRIMARY KEY,               -- Surrogate key (auto-increment)
    branch_id       VARCHAR(10) NOT NULL UNIQUE,       -- Business key: BR001
    branch_name     VARCHAR(100) NOT NULL,
    city            VARCHAR(50) NOT NULL,
    region          VARCHAR(50) NOT NULL,              -- North, Central, South
    branch_type     VARCHAR(20) NOT NULL,              -- Head Office, Branch, Sub-branch
    opening_date    DATE NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    last_modified   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);


-- ============================================================================
-- DIM_ACCOUNT_TYPE: Account Type Reference Dimension
-- ============================================================================
-- LEARNING: A "mini-dimension" or reference table. Small, rarely changes.
-- Contains the types of bank accounts available.
CREATE TABLE IF NOT EXISTS dim_account_type (
    account_type_key   SERIAL PRIMARY KEY,
    account_type_code  VARCHAR(10) NOT NULL UNIQUE,    -- SAV, CHK, FD, etc.
    account_type_name  VARCHAR(50) NOT NULL,
    category           VARCHAR(30) NOT NULL,           -- Deposit, Credit, Investment
    interest_rate      NUMERIC(5,2) DEFAULT 0,
    min_balance        NUMERIC(18,2) DEFAULT 0,
    is_active          BOOLEAN DEFAULT TRUE,
    last_modified      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);


-- ============================================================================
-- DIM_CUSTOMER: Customer Dimension (SCD Type 2)
-- ============================================================================
-- LEARNING: This is the most important dimension for SCD Type 2 understanding.
--
-- SCD Type 2 tracks HISTORY of changes:
--   - Each customer can have MULTIPLE rows (one per "version")
--   - is_current = TRUE → latest version
--   - effective_date / expiry_date → when this version was valid
--
-- Example: Customer changes address
--   Row 1: customer_id=CUST001, address="Old", is_current=FALSE, expiry_date=2024-03-15
--   Row 2: customer_id=CUST001, address="New", is_current=TRUE,  expiry_date=9999-12-31
--
-- The CUSTOMER_KEY (surrogate key) is unique per ROW.
-- The CUSTOMER_ID (business key) is unique per CUSTOMER (but has multiple rows).
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,              -- Business key: CUST000001
    full_name       VARCHAR(100) NOT NULL,
    date_of_birth   DATE,
    email           VARCHAR(100),
    phone           VARCHAR(20),
    address         VARCHAR(200),
    city            VARCHAR(50),
    region          VARCHAR(50),                       -- North, Central, South
    segment         VARCHAR(30) NOT NULL,              -- Mass, Affluent, VIP, Premium, Corporate
    registration_date DATE NOT NULL,
    kyc_status      VARCHAR(20) DEFAULT 'Verified',    -- Verified, Pending, Expired
    risk_score      INTEGER DEFAULT 50,                -- 0-100, higher = riskier
    -- SCD Type 2 fields
    effective_date  DATE NOT NULL,
    expiry_date     DATE DEFAULT '9999-12-31' NOT NULL,
    is_current      BOOLEAN DEFAULT TRUE NOT NULL,
    -- CDC tracking
    last_modified   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Index for efficient SCD Type 2 lookups
CREATE INDEX IF NOT EXISTS idx_customer_business_key ON dim_customer(customer_id, is_current);
CREATE INDEX IF NOT EXISTS idx_customer_cdc ON dim_customer(last_modified);


-- ============================================================================
-- DIM_ACCOUNT: Account Dimension
-- ============================================================================
-- LEARNING: Stores account details bridging customers and transactions.
CREATE TABLE IF NOT EXISTS dim_account (
    account_key       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id        VARCHAR(20) NOT NULL UNIQUE,       -- Business key: ACCT000001
    customer_id       VARCHAR(20) NOT NULL,              -- FK to dim_customer (business key)
    branch_id         VARCHAR(10) NOT NULL,              -- FK to dim_branch (business key)
    account_type_code VARCHAR(10) NOT NULL,              -- FK to dim_account_type (business key)
    balance           NUMERIC(18,2) NOT NULL,
    currency          VARCHAR(3) DEFAULT 'VND',
    status            VARCHAR(20) DEFAULT 'Active',      -- Active, Dormant, Closed, Frozen
    opened_date       DATE NOT NULL,
    last_activity_date DATE NOT NULL,
    last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_customer ON dim_account(customer_id);
CREATE INDEX IF NOT EXISTS idx_account_cdc ON dim_account(last_modified);


-- ============================================================================
-- FACT_TRANSACTION: Banking Transaction Fact Table
-- ============================================================================
-- LEARNING: The fact table is the heart of the data warehouse.
-- Each row = one banking transaction (deposit, withdrawal, transfer, etc.)
--
-- Grain: One row per transaction
-- Measures: amount (the numeric value we aggregate)
-- Foreign keys: customer_id, branch_id, account_type_code, txn_date_key

DROP TABLE IF EXISTS fact_transaction CASCADE;

CREATE TABLE IF NOT EXISTS fact_transaction (
    txn_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_id          VARCHAR(30) NOT NULL UNIQUE,       -- Business key: TXN20240315000001
    customer_id     VARCHAR(20) NOT NULL,              -- FK to dim_customer (business key)
    account_id      VARCHAR(20) NOT NULL,              -- FK to dim_account (business key)
    branch_id       VARCHAR(10) NOT NULL,              -- FK to dim_branch
    account_type_code VARCHAR(10) NOT NULL,            -- FK to dim_account_type
    txn_date_key    INTEGER NOT NULL,                  -- FK to dim_date (YYYYMMDD)
    txn_datetime    TIMESTAMP NOT NULL,                -- Exact transaction time
    txn_type        VARCHAR(20) NOT NULL,              -- Deposit, Withdrawal, Transfer, Payment
    amount          NUMERIC(18,2) NOT NULL,            -- Transaction amount in VND
    currency        VARCHAR(3) DEFAULT 'VND',
    channel         VARCHAR(20) NOT NULL,              -- ATM, Branch, Mobile, Internet, POS
    status          VARCHAR(20) DEFAULT 'Completed',   -- Completed, Pending, Failed
    description     VARCHAR(200),
    -- CDC tracking
    last_modified   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_txn_date ON fact_transaction(txn_date_key);
CREATE INDEX IF NOT EXISTS idx_txn_customer ON fact_transaction(customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_cdc ON fact_transaction(last_modified);


-- ============================================================================
-- FACT_DAILY_BALANCE: Daily Balance Snapshot Fact Table
-- ============================================================================
-- LEARNING: This is a "periodic snapshot" fact table (not transactional).
-- It captures the state of each account at end-of-day.
-- Useful for: balance trend analysis, regulatory reporting, risk monitoring.
CREATE TABLE IF NOT EXISTS fact_daily_balance (
    balance_key       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id       VARCHAR(20) NOT NULL,
    account_type_code VARCHAR(10) NOT NULL,
    date_key          INTEGER NOT NULL,                -- YYYYMMDD
    opening_balance   NUMERIC(18,2) NOT NULL,
    closing_balance   NUMERIC(18,2) NOT NULL,
    total_credits     NUMERIC(18,2) DEFAULT 0,
    total_debits      NUMERIC(18,2) DEFAULT 0,
    txn_count         INTEGER DEFAULT 0,
    last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_balance_date ON fact_daily_balance(date_key);
CREATE INDEX IF NOT EXISTS idx_balance_customer ON fact_daily_balance(customer_id);
CREATE INDEX IF NOT EXISTS idx_balance_cdc ON fact_daily_balance(last_modified);


-- ============================================================================
-- CDC_WATERMARK: Change Data Capture Tracking Table
-- ============================================================================
-- LEARNING: This is a SYSTEM table, not a business table.
-- It tracks the "high-water mark" for each table:
--   "What was the last LAST_MODIFIED timestamp we successfully extracted?"
--
-- The incremental pipeline reads this, extracts rows with
-- LAST_MODIFIED > watermark, processes them, then updates the watermark.
CREATE TABLE IF NOT EXISTS cdc_watermark (
    table_name      VARCHAR(50) PRIMARY KEY,
    last_watermark  TIMESTAMP NOT NULL DEFAULT '2020-01-01 00:00:00',
    last_row_count  INTEGER DEFAULT 0,
    last_run_status VARCHAR(20) DEFAULT 'PENDING',
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial watermark values
INSERT INTO cdc_watermark (table_name, last_watermark) VALUES
    ('dim_customer', '2020-01-01 00:00:00'),
    ('dim_account', '2020-01-01 00:00:00'),
    ('fact_transaction', '2020-01-01 00:00:00'),
    ('fact_daily_balance', '2020-01-01 00:00:00'),
    ('dim_branch', '2020-01-01 00:00:00'),
    ('dim_date', '2020-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;
