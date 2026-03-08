--------------------------------------------------------------------------------
-- Reporting & Analysis Tables + Population Procedures
-- ===============================================================
-- Banking simulation analytics layer for the Spark-ling project.
--
-- ARCHITECTURE:
--   Source layer  : fact_transaction, fact_daily_balance, dim_account,
--                   dim_customer, dim_branch, dim_account_type, dim_date
--   Reporting layer (this file):
--     rpt_monthly_txn_summary       Monthly transaction volume per customer/type
--     rpt_account_balance_snapshot  End-of-day account balances with tiering
--     rpt_customer_segment_kpi      Segment-level KPIs for management reporting
--     rpt_channel_analysis          Channel mix — digital vs traditional
--     rpt_dormant_watchlist         Inactive accounts for risk/compliance team
--
-- REFRESH STRATEGY: Full replace per reporting period (DELETE + INSERT).
--   This keeps procedures idempotent — safe to re-run without duplicates.
--   In production, incremental merge (UPSERT) is preferred for large tables;
--   full replace is acceptable here because:
--     a) Dataset is synthetic / bounded in size
--     b) Simplicity over micro-optimisation for a learning simulation
--
-- MASTER PROCEDURE: sp_run_daily_reporting()
--   Orchestrates all steps in dependency order.
--   Call this from AWS Lambda / Glue trigger after the daily ETL completes.
--
-- USAGE:
--   CALL sp_run_daily_reporting();                     -- today
--   CALL sp_run_daily_reporting('2025-06-30'::DATE);   -- historical backfill
--------------------------------------------------------------------------------


-- ============================================================================
-- TABLE 1: rpt_monthly_txn_summary
-- ============================================================================
-- Grain   : One row per (report_month × customer_id × account_type_code)
-- Sources : fact_transaction, dim_customer
-- Purpose :
--   - RM (Relationship Manager) activity scorecards
--   - Customer activity scoring for churn prediction
--   - Fee revenue estimation (Payment + Fee type transactions)
CREATE TABLE IF NOT EXISTS rpt_monthly_txn_summary (
    summary_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_month        INTEGER       NOT NULL,   -- YYYYMM format, e.g. 202503
    customer_id         VARCHAR(20)   NOT NULL,
    segment             VARCHAR(30),
    account_type_code   VARCHAR(10),
    txn_count           INTEGER       DEFAULT 0,
    total_credit_vnd    NUMERIC(22,2) DEFAULT 0,  -- Deposit + Transfer In + Interest
    total_debit_vnd     NUMERIC(22,2) DEFAULT 0,  -- Withdrawal + Transfer Out + Payment + Fee
    net_flow_vnd        NUMERIC(22,2) DEFAULT 0,  -- total_credit - total_debit
    avg_txn_amount      NUMERIC(18,2) DEFAULT 0,
    max_single_txn      NUMERIC(18,2) DEFAULT 0,  -- Largest single transaction (fraud signal)
    preferred_channel   VARCHAR(20),              -- Most-used channel this month
    last_modified       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rpt_monthly_month    ON rpt_monthly_txn_summary(report_month);
CREATE INDEX IF NOT EXISTS idx_rpt_monthly_customer ON rpt_monthly_txn_summary(customer_id);
CREATE INDEX IF NOT EXISTS idx_rpt_monthly_segment  ON rpt_monthly_txn_summary(segment);


-- ============================================================================
-- TABLE 2: rpt_account_balance_snapshot
-- ============================================================================
-- Grain   : One row per (snapshot_date × account_id)
-- Sources : dim_account, dim_customer, fact_daily_balance (fallback: dim_account.current_balance)
-- Purpose :
--   - Portfolio AUM (Assets Under Management) reporting to senior management
--   - NIM (Net Interest Margin) estimation by account type
--   - Balance tier distribution for product targeting
--
-- Balance tier thresholds (VND — Vietnamese Dong):
--   Micro    : < 1,000,000 VND      (< ~$40 USD)
--   Retail   : 1M – 100M VND        ($40 – $4,000)
--   Affluent : 100M – 1B VND        ($4K – $40K)
--   Private  : > 1B VND             (> $40K)
CREATE TABLE IF NOT EXISTS rpt_account_balance_snapshot (
    snapshot_key        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date       DATE          NOT NULL,
    account_id          VARCHAR(20)   NOT NULL,
    customer_id         VARCHAR(20)   NOT NULL,
    account_type_code   VARCHAR(10),
    segment             VARCHAR(30),
    branch_id           VARCHAR(10),
    current_balance     NUMERIC(18,2) DEFAULT 0,
    balance_tier        VARCHAR(20),   -- Micro | Retail | Affluent | Private
    status              VARCHAR(20),
    opened_date         DATE,
    days_since_activity INTEGER,       -- v_snap_date - last_activity_date
    last_modified       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rpt_snapshot_date    ON rpt_account_balance_snapshot(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_rpt_snapshot_account ON rpt_account_balance_snapshot(account_id);
CREATE INDEX IF NOT EXISTS idx_rpt_snapshot_segment ON rpt_account_balance_snapshot(segment);
CREATE INDEX IF NOT EXISTS idx_rpt_snapshot_tier    ON rpt_account_balance_snapshot(balance_tier);


-- ============================================================================
-- TABLE 3: rpt_customer_segment_kpi
-- ============================================================================
-- Grain   : One row per (report_month × segment)
-- Sources : dim_customer, dim_account, fact_transaction
-- Purpose :
--   - Monthly executive dashboard (CEO / CFO view)
--   - Segment migration tracking (e.g., Mass → Mass Affluent)
--   - Revenue allocation per segment for pricing decisions
CREATE TABLE IF NOT EXISTS rpt_customer_segment_kpi (
    kpi_key                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_month            INTEGER       NOT NULL,
    segment                 VARCHAR(30)   NOT NULL,
    total_customers         INTEGER       DEFAULT 0,
    total_accounts          INTEGER       DEFAULT 0,
    active_accounts         INTEGER       DEFAULT 0,   -- status = 'Active'
    total_balance_vnd       NUMERIC(22,2) DEFAULT 0,   -- AUM proxy across all accounts
    avg_balance_per_acct    NUMERIC(18,2) DEFAULT 0,
    avg_balance_per_cust    NUMERIC(18,2) DEFAULT 0,   -- Wealth per customer head
    total_txn_volume_vnd    NUMERIC(22,2) DEFAULT 0,   -- Transaction throughput this month
    txn_count               INTEGER       DEFAULT 0,
    avg_txn_per_customer    NUMERIC(10,2) DEFAULT 0,   -- Engagement metric
    dormant_account_pct     NUMERIC(6,2)  DEFAULT 0,   -- % Dormant or Frozen accounts
    last_modified           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rpt_kpi_month_segment
    ON rpt_customer_segment_kpi(report_month, segment);


-- ============================================================================
-- TABLE 4: rpt_channel_analysis
-- ============================================================================
-- Grain   : One row per (report_month × channel)
-- Sources : fact_transaction
-- Purpose :
--   - Digital transformation KPI tracking (Mobile App + Internet Banking share)
--   - ATM vs Branch cost-to-serve analysis
--   - API channel growth (fintech / open banking)
--
-- Channels from data_generator.py: Branch, ATM, Mobile App, Internet Banking, POS, API
-- Digital channels flagged TRUE : Mobile App, Internet Banking, API
CREATE TABLE IF NOT EXISTS rpt_channel_analysis (
    channel_key         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_month        INTEGER       NOT NULL,
    channel             VARCHAR(20)   NOT NULL,
    txn_count           INTEGER       DEFAULT 0,
    total_volume_vnd    NUMERIC(22,2) DEFAULT 0,
    avg_txn_amount      NUMERIC(18,2) DEFAULT 0,
    pct_of_total_txns   NUMERIC(6,2)  DEFAULT 0,  -- Share of monthly transaction count
    pct_of_total_vol    NUMERIC(6,2)  DEFAULT 0,  -- Share of monthly transaction volume
    digital_flag        BOOLEAN       DEFAULT FALSE,
    last_modified       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rpt_channel_month
    ON rpt_channel_analysis(report_month, channel);


-- ============================================================================
-- TABLE 5: rpt_dormant_watchlist
-- ============================================================================
-- Grain   : One row per (snapshot_date × account_id)
-- Sources : dim_account, dim_customer
-- Purpose :
--   - SBV (State Bank of Vietnam) regulatory compliance
--     SBV Circular 14/2017: formally dormant after 12 months of inactivity
--   - Early warning system — flag accounts at 60+ days for proactive outreach
--   - Input for RM task lists and SMS/push reactivation campaigns
--
-- Dormancy tiers:
--   At Risk      :  60–89 days inactive  → send reactivation nudge
--   Pre-Dormant  :  90–179 days inactive → RM outreach; fee waiver offer
--   Dormant      :  180+ days inactive   → initiate SBV dormant account process
CREATE TABLE IF NOT EXISTS rpt_dormant_watchlist (
    watchlist_key       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date       DATE          NOT NULL,
    account_id          VARCHAR(20)   NOT NULL,
    customer_id         VARCHAR(20)   NOT NULL,
    customer_name       VARCHAR(100),
    segment             VARCHAR(30),
    account_type_code   VARCHAR(10),
    branch_id           VARCHAR(10),
    current_balance     NUMERIC(18,2) DEFAULT 0,
    last_activity_date  DATE,
    days_inactive       INTEGER,
    dormancy_risk       VARCHAR(20),   -- At Risk | Pre-Dormant | Dormant
    recommended_action  VARCHAR(150),
    last_modified       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rpt_dormant_date    ON rpt_dormant_watchlist(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_rpt_dormant_account ON rpt_dormant_watchlist(account_id);
CREATE INDEX IF NOT EXISTS idx_rpt_dormant_risk    ON rpt_dormant_watchlist(dormancy_risk);


-- ============================================================================
-- PROCEDURE 1: sp_populate_rpt_monthly_txn_summary
-- ============================================================================
-- Parameters:
--   p_report_month  INTEGER  YYYYMM format; defaults to current month.
-- Notes:
--   - Only counts status = 'Completed' transactions (excludes Failed/Reversed)
--   - preferred_channel uses DISTINCT ON + ORDER BY COUNT() DESC to pick the
--     most-used channel per (customer, account_type) within the month
CREATE OR REPLACE PROCEDURE sp_populate_rpt_monthly_txn_summary(
    p_report_month INTEGER DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_month INTEGER;
    v_affected     INTEGER := 0;
    v_start_date   DATE;
    v_end_date     DATE;
BEGIN
    v_target_month := COALESCE(p_report_month, TO_CHAR(CURRENT_DATE, 'YYYYMM')::INTEGER);
    v_start_date   := TO_DATE(v_target_month::TEXT || '01', 'YYYYMMDD');
    v_end_date     := (v_start_date + INTERVAL '1 month - 1 day')::DATE;

    RAISE NOTICE '[sp_populate_rpt_monthly_txn_summary] Processing month: %', v_target_month;

    -- Idempotent: remove existing rows for this period before re-inserting
    DELETE FROM rpt_monthly_txn_summary WHERE report_month = v_target_month;

    INSERT INTO rpt_monthly_txn_summary (
        report_month, customer_id, segment, account_type_code,
        txn_count, total_credit_vnd, total_debit_vnd, net_flow_vnd,
        avg_txn_amount, max_single_txn, preferred_channel, last_modified
    )
    WITH txn_base AS (
        -- Join transactions to current customer record for segment
        SELECT
            ft.customer_id,
            ft.account_type_code,
            ft.channel,
            ft.txn_type,
            ft.amount
        FROM fact_transaction ft
        JOIN dim_customer     dc ON dc.customer_id = ft.customer_id
                                AND dc.is_current  = TRUE
        WHERE ft.txn_datetime >= v_start_date
          AND ft.txn_datetime <  v_end_date + INTERVAL '1 day'
          AND ft.status = 'Completed'
    ),
    channel_rank AS (
        -- Rank channels by usage frequency per customer + account_type
        SELECT
            customer_id,
            account_type_code,
            channel,
            COUNT(*) AS channel_count,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id, account_type_code
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM txn_base
        GROUP BY customer_id, account_type_code, channel
    )
    SELECT
        v_target_month,
        tb.customer_id,
        MAX(dc.segment)                                                     AS segment,
        tb.account_type_code,
        COUNT(*)                                                            AS txn_count,
        SUM(CASE WHEN tb.txn_type IN ('Deposit','Transfer In','Interest')
                 THEN tb.amount ELSE 0 END)                                AS total_credit_vnd,
        SUM(CASE WHEN tb.txn_type IN ('Withdrawal','Transfer Out','Payment','Fee')
                 THEN tb.amount ELSE 0 END)                                AS total_debit_vnd,
        SUM(CASE
            WHEN tb.txn_type IN ('Deposit','Transfer In','Interest')       THEN  tb.amount
            WHEN tb.txn_type IN ('Withdrawal','Transfer Out','Payment','Fee') THEN -tb.amount
            ELSE 0 END)                                                    AS net_flow_vnd,
        ROUND(AVG(tb.amount), 2)                                           AS avg_txn_amount,
        MAX(tb.amount)                                                     AS max_single_txn,
        MAX(cr.channel) FILTER (WHERE cr.rn = 1)                           AS preferred_channel,
        CURRENT_TIMESTAMP
    FROM txn_base tb
    JOIN dim_customer dc ON dc.customer_id = tb.customer_id AND dc.is_current = TRUE
    LEFT JOIN channel_rank cr
           ON cr.customer_id      = tb.customer_id
          AND cr.account_type_code = tb.account_type_code
          AND cr.rn               = 1
    GROUP BY tb.customer_id, tb.account_type_code;

    GET DIAGNOSTICS v_affected = ROW_COUNT;
    RAISE NOTICE '[sp_populate_rpt_monthly_txn_summary] % rows for month %', v_affected, v_target_month;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[sp_populate_rpt_monthly_txn_summary] Failed: %', SQLERRM;
END;
$$;


-- ============================================================================
-- PROCEDURE 2: sp_populate_rpt_account_balance_snapshot
-- ============================================================================
-- Parameters:
--   p_snapshot_date  DATE  Defaults to today. Used for historical backfill.
-- Notes:
--   - Joins fact_daily_balance first (most accurate if ETL ran)
--   - Falls back to dim_account.current_balance (SCD1 last-known value)
--   - Excludes accounts with status = 'Closed'
CREATE OR REPLACE PROCEDURE sp_populate_rpt_account_balance_snapshot(
    p_snapshot_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_snap_date DATE;
    v_affected  INTEGER := 0;
BEGIN
    v_snap_date := COALESCE(p_snapshot_date, CURRENT_DATE);

    RAISE NOTICE '[sp_populate_rpt_account_balance_snapshot] Snapshot date: %', v_snap_date;

    DELETE FROM rpt_account_balance_snapshot WHERE snapshot_date = v_snap_date;

    INSERT INTO rpt_account_balance_snapshot (
        snapshot_date, account_id, customer_id, account_type_code, segment,
        branch_id, current_balance, balance_tier, status,
        opened_date, days_since_activity, last_modified
    )
    SELECT
        v_snap_date,
        da.account_id,
        da.customer_id,
        da.account_type_code,
        dc.segment,
        da.branch_id,
        -- Prefer end-of-day balance from fact table; fall back to dimension
        COALESCE(fdb.closing_balance, da.current_balance)              AS current_balance,
        CASE
            WHEN COALESCE(fdb.closing_balance, da.current_balance) < 1000000         THEN 'Micro'
            WHEN COALESCE(fdb.closing_balance, da.current_balance) < 100000000       THEN 'Retail'
            WHEN COALESCE(fdb.closing_balance, da.current_balance) < 1000000000      THEN 'Affluent'
            ELSE                                                                           'Private'
        END                                                            AS balance_tier,
        da.status,
        da.opened_date,
        CASE
            WHEN da.last_activity_date IS NOT NULL
            THEN (v_snap_date - da.last_activity_date)
            ELSE NULL
        END                                                            AS days_since_activity,
        CURRENT_TIMESTAMP
    FROM dim_account     da
    JOIN dim_customer    dc  ON dc.customer_id = da.customer_id
                            AND dc.is_current  = TRUE
    LEFT JOIN fact_daily_balance fdb
           ON fdb.customer_id       = da.customer_id
          AND fdb.account_type_code = da.account_type_code
          AND fdb.date_key          = TO_CHAR(v_snap_date, 'YYYYMMDD')::INTEGER
    WHERE da.status <> 'Closed';

    GET DIAGNOSTICS v_affected = ROW_COUNT;
    RAISE NOTICE '[sp_populate_rpt_account_balance_snapshot] % rows for %', v_affected, v_snap_date;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[sp_populate_rpt_account_balance_snapshot] Failed: %', SQLERRM;
END;
$$;


-- ============================================================================
-- PROCEDURE 3: sp_populate_rpt_customer_segment_kpi
-- ============================================================================
-- Parameters:
--   p_report_month  INTEGER  YYYYMM; defaults to current month.
-- Notes:
--   - account_stats CTE operates on ALL accounts (current snapshot, not month-bound)
--   - txn_stats CTE is scoped to the given month (Completed only)
--   - UNIQUE constraint on (report_month, segment) prevents duplicates
CREATE OR REPLACE PROCEDURE sp_populate_rpt_customer_segment_kpi(
    p_report_month INTEGER DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_month INTEGER;
    v_affected     INTEGER := 0;
    v_start_date   DATE;
    v_end_date     DATE;
BEGIN
    v_target_month := COALESCE(p_report_month, TO_CHAR(CURRENT_DATE, 'YYYYMM')::INTEGER);
    v_start_date   := TO_DATE(v_target_month::TEXT || '01', 'YYYYMMDD');
    v_end_date     := (v_start_date + INTERVAL '1 month - 1 day')::DATE;

    RAISE NOTICE '[sp_populate_rpt_customer_segment_kpi] Month: %', v_target_month;

    DELETE FROM rpt_customer_segment_kpi WHERE report_month = v_target_month;

    INSERT INTO rpt_customer_segment_kpi (
        report_month, segment,
        total_customers, total_accounts, active_accounts,
        total_balance_vnd, avg_balance_per_acct, avg_balance_per_cust,
        total_txn_volume_vnd, txn_count, avg_txn_per_customer,
        dormant_account_pct, last_modified
    )
    WITH account_stats AS (
        SELECT
            dc.segment,
            COUNT(DISTINCT dc.customer_id)                                       AS total_customers,
            COUNT(da.account_id)                                                 AS total_accounts,
            COUNT(da.account_id) FILTER (WHERE da.status = 'Active')            AS active_accounts,
            COUNT(da.account_id) FILTER (WHERE da.status IN ('Dormant','Frozen')) AS dormant_accounts,
            SUM(da.current_balance)                                              AS total_balance_vnd,
            AVG(da.current_balance)                                              AS avg_balance_per_acct,
            SUM(da.current_balance)
                / NULLIF(COUNT(DISTINCT dc.customer_id), 0)                     AS avg_balance_per_cust
        FROM dim_account  da
        JOIN dim_customer dc ON dc.customer_id = da.customer_id AND dc.is_current = TRUE
        WHERE da.status <> 'Closed'
        GROUP BY dc.segment
    ),
    txn_stats AS (
        SELECT
            dc.segment,
            SUM(ft.amount)                                                       AS total_txn_volume_vnd,
            COUNT(ft.txn_key)                                                    AS txn_count,
            COUNT(ft.txn_key)::NUMERIC
                / NULLIF(COUNT(DISTINCT ft.customer_id), 0)                     AS avg_txn_per_customer
        FROM fact_transaction ft
        JOIN dim_customer     dc ON dc.customer_id = ft.customer_id AND dc.is_current = TRUE
        WHERE ft.txn_datetime >= v_start_date
          AND ft.txn_datetime <  v_end_date + INTERVAL '1 day'
          AND ft.status = 'Completed'
        GROUP BY dc.segment
    )
    SELECT
        v_target_month,
        a.segment,
        a.total_customers,
        a.total_accounts,
        a.active_accounts,
        a.total_balance_vnd,
        ROUND(a.avg_balance_per_acct, 2),
        ROUND(a.avg_balance_per_cust, 2),
        COALESCE(t.total_txn_volume_vnd, 0),
        COALESCE(t.txn_count,            0),
        ROUND(COALESCE(t.avg_txn_per_customer, 0), 2),
        ROUND(a.dormant_accounts::NUMERIC / NULLIF(a.total_accounts, 0) * 100, 2),
        CURRENT_TIMESTAMP
    FROM account_stats  a
    LEFT JOIN txn_stats t ON t.segment = a.segment;

    GET DIAGNOSTICS v_affected = ROW_COUNT;
    RAISE NOTICE '[sp_populate_rpt_customer_segment_kpi] % rows for month %', v_affected, v_target_month;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[sp_populate_rpt_customer_segment_kpi] Failed: %', SQLERRM;
END;
$$;


-- ============================================================================
-- PROCEDURE 4: sp_populate_rpt_channel_analysis
-- ============================================================================
-- Parameters:
--   p_report_month  INTEGER  YYYYMM; defaults to current month.
-- Notes:
--   - CROSS JOIN totals computes share percentages in a single pass
--   - digital_flag = TRUE for channels that indicate self-service adoption
CREATE OR REPLACE PROCEDURE sp_populate_rpt_channel_analysis(
    p_report_month INTEGER DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_month INTEGER;
    v_affected     INTEGER := 0;
    v_start_date   DATE;
    v_end_date     DATE;
BEGIN
    v_target_month := COALESCE(p_report_month, TO_CHAR(CURRENT_DATE, 'YYYYMM')::INTEGER);
    v_start_date   := TO_DATE(v_target_month::TEXT || '01', 'YYYYMMDD');
    v_end_date     := (v_start_date + INTERVAL '1 month - 1 day')::DATE;

    RAISE NOTICE '[sp_populate_rpt_channel_analysis] Month: %', v_target_month;

    DELETE FROM rpt_channel_analysis WHERE report_month = v_target_month;

    INSERT INTO rpt_channel_analysis (
        report_month, channel, txn_count, total_volume_vnd,
        avg_txn_amount, pct_of_total_txns, pct_of_total_vol,
        digital_flag, last_modified
    )
    WITH channel_base AS (
        SELECT
            channel,
            COUNT(*)    AS txn_count,
            SUM(amount) AS total_volume_vnd,
            AVG(amount) AS avg_txn_amount
        FROM fact_transaction
        WHERE txn_datetime >= v_start_date
          AND txn_datetime <  v_end_date + INTERVAL '1 day'
          AND status = 'Completed'
        GROUP BY channel
    ),
    totals AS (
        SELECT
            SUM(txn_count)        AS grand_txn_count,
            SUM(total_volume_vnd) AS grand_volume
        FROM channel_base
    )
    SELECT
        v_target_month,
        cb.channel,
        cb.txn_count,
        cb.total_volume_vnd,
        ROUND(cb.avg_txn_amount, 2),
        ROUND(cb.txn_count::NUMERIC        / NULLIF(t.grand_txn_count, 0) * 100, 2) AS pct_of_total_txns,
        ROUND(cb.total_volume_vnd::NUMERIC / NULLIF(t.grand_volume,    0) * 100, 2) AS pct_of_total_vol,
        CASE cb.channel
            WHEN 'Mobile App'       THEN TRUE
            WHEN 'Internet Banking' THEN TRUE
            WHEN 'API'              THEN TRUE
            ELSE FALSE
        END AS digital_flag,
        CURRENT_TIMESTAMP
    FROM channel_base cb
    CROSS JOIN totals t;

    GET DIAGNOSTICS v_affected = ROW_COUNT;
    RAISE NOTICE '[sp_populate_rpt_channel_analysis] % channels processed for month %', v_affected, v_target_month;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[sp_populate_rpt_channel_analysis] Failed: %', SQLERRM;
END;
$$;


-- ============================================================================
-- PROCEDURE 5: sp_populate_rpt_dormant_watchlist
-- ============================================================================
-- Parameters:
--   p_snapshot_date      DATE     Defaults to today.
--   p_min_days_inactive  INTEGER  Minimum days inactive to include; default 60.
-- Notes:
--   - Only flags Active and Dormant accounts (Closed/Frozen handled separately)
--   - recommended_action provides human-readable next step for RM/ops team
--   - SBV Circular 14/2017 defines 12-month threshold for official dormancy
CREATE OR REPLACE PROCEDURE sp_populate_rpt_dormant_watchlist(
    p_snapshot_date     DATE    DEFAULT NULL,
    p_min_days_inactive INTEGER DEFAULT 60
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_snap_date DATE;
    v_affected  INTEGER := 0;
BEGIN
    v_snap_date := COALESCE(p_snapshot_date, CURRENT_DATE);

    RAISE NOTICE '[sp_populate_rpt_dormant_watchlist] Snapshot: %, Min inactive days: %',
        v_snap_date, p_min_days_inactive;

    DELETE FROM rpt_dormant_watchlist WHERE snapshot_date = v_snap_date;

    INSERT INTO rpt_dormant_watchlist (
        snapshot_date, account_id, customer_id, customer_name,
        segment, account_type_code, branch_id,
        current_balance, last_activity_date, days_inactive,
        dormancy_risk, recommended_action, last_modified
    )
    SELECT
        v_snap_date,
        da.account_id,
        da.customer_id,
        dc.full_name,
        dc.segment,
        da.account_type_code,
        da.branch_id,
        da.current_balance,
        da.last_activity_date,
        (v_snap_date - da.last_activity_date)                           AS days_inactive,
        CASE
            WHEN (v_snap_date - da.last_activity_date) BETWEEN p_min_days_inactive AND 89
                THEN 'At Risk'
            WHEN (v_snap_date - da.last_activity_date) BETWEEN 90 AND 179
                THEN 'Pre-Dormant'
            WHEN (v_snap_date - da.last_activity_date) >= 180
                THEN 'Dormant'
        END                                                             AS dormancy_risk,
        CASE
            WHEN (v_snap_date - da.last_activity_date) BETWEEN p_min_days_inactive AND 89
                THEN 'Send reactivation SMS / push notification'
            WHEN (v_snap_date - da.last_activity_date) BETWEEN 90 AND 179
                THEN 'Assign RM outreach; offer fee waiver for next cycle'
            WHEN (v_snap_date - da.last_activity_date) >= 180
                THEN 'Initiate SBV dormant account process; consider full fee waiver'
        END                                                             AS recommended_action,
        CURRENT_TIMESTAMP
    FROM dim_account  da
    JOIN dim_customer dc ON dc.customer_id = da.customer_id
                        AND dc.is_current  = TRUE
    WHERE da.status IN ('Active', 'Dormant')
      AND da.last_activity_date IS NOT NULL
      AND (v_snap_date - da.last_activity_date) >= p_min_days_inactive;

    GET DIAGNOSTICS v_affected = ROW_COUNT;
    RAISE NOTICE '[sp_populate_rpt_dormant_watchlist] % accounts flagged', v_affected;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[sp_populate_rpt_dormant_watchlist] Failed: %', SQLERRM;
END;
$$;


-- ============================================================================
-- MASTER PROCEDURE: sp_run_daily_reporting
-- ============================================================================
-- Orchestrates all reporting procedures in dependency order.
-- Designed to be invoked by the AWS Lambda / Glue daily trigger
-- after the nightly ETL pipeline has completed.
--
-- Execution order:
--   Step 1 : sp_populate_dim_account                     — must run first
--   Step 2 : sp_populate_rpt_account_balance_snapshot    — depends on dim_account
--   Step 3 : sp_populate_rpt_monthly_txn_summary         — depends on fact_transaction
--   Step 4 : sp_populate_rpt_customer_segment_kpi        — depends on dim_account + fact_transaction
--   Step 5 : sp_populate_rpt_channel_analysis            — depends on fact_transaction only
--   Step 6 : sp_populate_rpt_dormant_watchlist           — depends on dim_account
--
-- Parameters:
--   p_run_date  DATE  Target date for snapshots; month is derived automatically.
--                     Defaults to CURRENT_DATE — safe for daily scheduler.
--
-- Usage:
--   CALL sp_run_daily_reporting();                       -- today (scheduled run)
--   CALL sp_run_daily_reporting('2025-03-31'::DATE);     -- backfill end-of-March
CREATE OR REPLACE PROCEDURE sp_run_daily_reporting(
    p_run_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_date     DATE    := COALESCE(p_run_date, CURRENT_DATE);
    v_report_month INTEGER := TO_CHAR(v_run_date, 'YYYYMM')::INTEGER;
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'sp_run_daily_reporting  START  |  date: %  |  month: %',
        v_run_date, v_report_month;
    RAISE NOTICE '============================================================';

    -- Step 1: Refresh account dimension (requires stg_accounts already loaded)
    CALL sp_populate_dim_account();

    -- Step 2: Balance snapshot (requires fresh dim_account)
    CALL sp_populate_rpt_account_balance_snapshot(v_run_date);

    -- Step 3-5: Monthly aggregates (idempotent — safe to re-run mid-month)
    CALL sp_populate_rpt_monthly_txn_summary(v_report_month);
    CALL sp_populate_rpt_customer_segment_kpi(v_report_month);
    CALL sp_populate_rpt_channel_analysis(v_report_month);

    -- Step 6: Dormant risk watchlist
    CALL sp_populate_rpt_dormant_watchlist(v_run_date);

    RAISE NOTICE '============================================================';
    RAISE NOTICE 'sp_run_daily_reporting  COMPLETE  at %', CURRENT_TIMESTAMP;
    RAISE NOTICE '============================================================';

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_run_daily_reporting FAILED: %', SQLERRM;
END;
$$;
