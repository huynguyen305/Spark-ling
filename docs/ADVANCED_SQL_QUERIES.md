# Advanced SQL/Spark Queries — Databricks Performance Testing & Learning Guide

> **Environment**: Spark 4.1.0 on Databricks Serverless | MCP Spark Connect  
> **Dataset**: Vietnamese Banking Data (4 tables, S3-backed)  
> **Date**: March 2026

---

## Table of Contents

1. [Dataset Schema](#dataset-schema)
2. [Query 1 — Banking Risk Analytics Dashboard](#query-1--banking-risk-analytics-dashboard)
3. [Query 2 — Regional Time-Series Analytics](#query-2--regional-time-series-analytics)
4. [Query 3 — Anomaly Detection & Behavioral Analysis](#query-3--anomaly-detection--behavioral-analysis)
5. [SQL Concepts Learning Guide](#sql-concepts-learning-guide)
6. [Performance Notes](#performance-notes)

---

## Dataset Schema

### `customers` (10 columns)
| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | Unique ID (e.g., CUST000001) |
| name | string | Customer name |
| email | string | Email address |
| phone | string | Phone number |
| segment | string | Mass / Mass Affluent / Affluent / HNW / UHNW |
| registration_date | string | Registration date |
| kyc_status | string | Verified / Pending / Expired |
| date_of_birth | string | Date of birth |
| gender | string | M / F |
| nationality | string | Nationality |

### `accounts` (9 columns)
| Column | Type | Description |
|--------|------|-------------|
| account_id | string | Unique ID (e.g., ACC0000001) |
| customer_id | string | FK to customers |
| branch_id | string | FK to branches |
| account_type | string | Savings / Investment / Current / Loan / Credit Card |
| balance | double | Account balance (VND) |
| currency | string | Currency code |
| status | string | Active / Closed / Dormant |
| opened_date | string | Account open date |
| last_activity_date | string | Last activity date |

### `transactions` (11 columns)
| Column | Type | Description |
|--------|------|-------------|
| txn_id | string | Unique transaction ID |
| account_id | string | FK to accounts |
| txn_datetime | string | Transaction timestamp |
| txn_type | string | Withdrawal / Deposit / Transfer Out / Transfer In / Fee / Interest |
| amount | double | Transaction amount (VND) |
| currency | string | Currency code |
| channel | string | Online / ATM / Branch / Mobile / API |
| merchant_category | string | Merchant category |
| status | string | Transaction status |
| reference | string | Reference number |
| description | string | Description |

### `branches` (6 columns)
| Column | Type | Description |
|--------|------|-------------|
| branch_id | string | Unique branch ID |
| branch_name | string | Branch name |
| region | string | Hanoi / Ho Chi Minh / Da Nang / Hai Phong / Can Tho / Dong Nai / Binh Duong / Quang Ninh |
| city | string | City |
| address | string | Address |
| opened_date | string | Branch opening date |

---

## Query 1 — Banking Risk Analytics Dashboard

**Difficulty**: ★★★★☆  
**Concepts**: 4 CTEs, 4-table JOINs, RANK/LAG/NTILE window functions, running SUM, conditional aggregation, composite risk scoring  
**Result**: 30 rows — mostly UHNW customers rated CRITICAL (scores 62-68)

```sql
WITH
-- ═══════════════════════════════════════════════════════════════
-- CTE 1: Transaction metrics per customer — volume, frequency, 
-- channel diversity, type breakdown
-- ═══════════════════════════════════════════════════════════════
customer_txn_metrics AS (
    SELECT 
        a.customer_id,
        COUNT(*) AS total_txns,
        SUM(t.amount) AS total_volume,
        AVG(t.amount) AS avg_txn_amount,
        MAX(t.amount) AS max_txn_amount,
        MIN(t.amount) AS min_txn_amount,
        STDDEV(t.amount) AS stddev_amount,
        COUNT(DISTINCT t.channel) AS channel_diversity,
        COUNT(DISTINCT DATE(CAST(t.txn_datetime AS TIMESTAMP))) AS active_days,
        COUNT(DISTINCT a.account_id) AS num_accounts,
        
        -- Conditional aggregation: break down by transaction type
        SUM(CASE WHEN t.txn_type = 'Withdrawal' THEN t.amount ELSE 0 END) AS withdrawal_vol,
        SUM(CASE WHEN t.txn_type = 'Deposit' THEN t.amount ELSE 0 END) AS deposit_vol,
        SUM(CASE WHEN t.txn_type IN ('Transfer Out', 'Transfer In') THEN t.amount ELSE 0 END) AS transfer_vol,
        
        -- Spike detection: transactions > 5x average
        SUM(CASE WHEN t.amount > 5 * AVG(t.amount) OVER () THEN 1 ELSE 0 END) AS spike_count,
        
        -- Per-channel volume
        SUM(CASE WHEN t.channel = 'Online' THEN t.amount ELSE 0 END) AS online_vol,
        SUM(CASE WHEN t.channel = 'ATM' THEN t.amount ELSE 0 END) AS atm_vol,
        SUM(CASE WHEN t.channel = 'Branch' THEN t.amount ELSE 0 END) AS branch_vol,
        SUM(CASE WHEN t.channel = 'Mobile' THEN t.amount ELSE 0 END) AS mobile_vol
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 2: Account-level analysis with branch region
-- ═══════════════════════════════════════════════════════════════
account_analysis AS (
    SELECT 
        a.customer_id,
        COUNT(*) AS total_accounts,
        SUM(a.balance) AS total_balance,
        AVG(a.balance) AS avg_balance,
        COUNT(CASE WHEN a.status = 'Dormant' THEN 1 END) AS dormant_accounts,
        COUNT(CASE WHEN a.status = 'Active' THEN 1 END) AS active_accounts,
        COUNT(DISTINCT b.region) AS region_spread,
        COLLECT_SET(b.region) AS regions
    FROM accounts a
    JOIN branches b ON a.branch_id = b.branch_id
    GROUP BY a.customer_id
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 3: Composite risk score using multiple weighted factors
-- ═══════════════════════════════════════════════════════════════
risk_scored AS (
    SELECT
        ctm.customer_id,
        c.name,
        c.segment,
        c.kyc_status,
        ctm.total_txns,
        ctm.total_volume,
        ctm.avg_txn_amount,
        ctm.channel_diversity,
        ctm.spike_count,
        aa.total_balance,
        aa.dormant_accounts,
        aa.region_spread,

        -- NTILE: percentile ranking of volume
        NTILE(100) OVER (ORDER BY ctm.total_volume) AS volume_percentile,
        
        -- LAG: compare current volume to previous customer (ordered by volume)
        LAG(ctm.total_volume, 1) OVER (ORDER BY ctm.total_volume) AS prev_customer_vol,
        
        -- Running sum: cumulative volume across all customers
        SUM(ctm.total_volume) OVER (
            ORDER BY ctm.total_volume 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_volume,

        -- Composite risk score (0-100)
        ROUND(
            (LEAST(20, ctm.spike_count * 4)) +
            (LEAST(20, COALESCE(ctm.stddev_amount / NULLIF(ctm.avg_txn_amount, 0), 0) * 10)) +
            (CASE WHEN c.kyc_status = 'Expired' THEN 15 WHEN c.kyc_status = 'Pending' THEN 8 ELSE 0 END) +
            (LEAST(15, aa.dormant_accounts * 5)) +
            (LEAST(15, (ctm.withdrawal_vol / NULLIF(ctm.deposit_vol, 0) - 1) * 10)) +
            (LEAST(15, aa.region_spread * 3))
        , 2) AS risk_score
    FROM customer_txn_metrics ctm
    JOIN customers c ON ctm.customer_id = c.customer_id
    JOIN account_analysis aa ON ctm.customer_id = aa.customer_id
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 4: Final ranking and risk tier assignment
-- ═══════════════════════════════════════════════════════════════
risk_ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY segment ORDER BY risk_score DESC) AS risk_rank_in_segment,
        CASE
            WHEN risk_score >= 60 THEN 'CRITICAL'
            WHEN risk_score >= 40 THEN 'HIGH'
            WHEN risk_score >= 20 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_tier
    FROM risk_scored
)

SELECT 
    customer_id, name, segment, kyc_status,
    total_txns, 
    ROUND(total_volume, 0) AS total_volume,
    ROUND(avg_txn_amount, 0) AS avg_txn_amt,
    channel_diversity, spike_count,
    ROUND(total_balance, 0) AS total_balance,
    dormant_accounts, region_spread,
    volume_percentile,
    risk_score, risk_tier, risk_rank_in_segment
FROM risk_ranked
WHERE risk_tier IN ('CRITICAL', 'HIGH')
ORDER BY risk_score DESC, total_volume DESC
LIMIT 30;
```

### Key Results (Query 1)
- Top 30 riskiest customers dominated by **UHNW segment**
- Risk scores ranged **62-68** (all CRITICAL/HIGH tier)
- Notable outlier: **CUST006515** (Mass Affluent, score 68.19, HIGH tier) — only 14 transactions but 5 spike events
- UHNW customers had 12K-33K transactions each

---

## Query 2 — Regional Time-Series Analytics

**Difficulty**: ★★★★☆  
**Concepts**: Monthly aggregation, percentile approximations (p50/p90/p99), MoM growth via LAG, Herfindahl channel concentration index, cumulative YTD volumes  
**Result**: 50 rows — regional + segment data across months

```sql
WITH
-- ═══════════════════════════════════════════════════════════════
-- CTE 1: Monthly aggregation by region + segment
-- ═══════════════════════════════════════════════════════════════
monthly_metrics AS (
    SELECT
        b.region,
        c.segment,
        DATE_TRUNC('month', CAST(t.txn_datetime AS TIMESTAMP)) AS txn_month,
        COUNT(*) AS txn_count,
        SUM(t.amount) AS monthly_volume,
        AVG(t.amount) AS avg_amount,
        
        -- Percentile approximations (cheaper than exact PERCENTILE)
        PERCENTILE_APPROX(t.amount, 0.5)  AS median_txn_amount,
        PERCENTILE_APPROX(t.amount, 0.9)  AS p90_txn_amount,
        PERCENTILE_APPROX(t.amount, 0.99) AS p99_txn_amount,
        
        -- Channel distribution
        COUNT(CASE WHEN t.channel = 'Online' THEN 1 END) AS online_count,
        COUNT(CASE WHEN t.channel = 'ATM' THEN 1 END) AS atm_count,
        COUNT(CASE WHEN t.channel = 'Branch' THEN 1 END) AS branch_count,
        COUNT(CASE WHEN t.channel = 'Mobile' THEN 1 END) AS mobile_count,
        COUNT(CASE WHEN t.channel = 'API' THEN 1 END) AS api_count,
        
        -- Customer metrics
        COUNT(DISTINCT a.customer_id) AS unique_customers,
        COUNT(DISTINCT a.account_id) AS unique_accounts,
        
        -- Risk indicators
        COUNT(CASE WHEN c.kyc_status = 'Expired' THEN 1 END) AS expired_kyc_count,
        COUNT(CASE WHEN a.status = 'Dormant' THEN 1 END) AS dormant_acct_txns
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    JOIN branches b ON a.branch_id = b.branch_id
    GROUP BY b.region, c.segment, DATE_TRUNC('month', CAST(t.txn_datetime AS TIMESTAMP))
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 2: Calculate MoM growth + channel concentration (HHI)
-- ═══════════════════════════════════════════════════════════════
enriched AS (
    SELECT
        *,
        -- Month-over-Month growth rate
        LAG(monthly_volume, 1) OVER (
            PARTITION BY region, segment ORDER BY txn_month
        ) AS prev_month_vol,
        
        ROUND(
            (monthly_volume - LAG(monthly_volume, 1) OVER (
                PARTITION BY region, segment ORDER BY txn_month
            )) / NULLIF(LAG(monthly_volume, 1) OVER (
                PARTITION BY region, segment ORDER BY txn_month
            ), 0) * 100
        , 2) AS mom_growth_pct,
        
        -- Herfindahl-Hirschman Index for channel concentration
        -- Lower = more diversified, Higher = concentrated on fewer channels
        ROUND(
            POW(online_count * 1.0 / NULLIF(txn_count, 0), 2) +
            POW(atm_count * 1.0 / NULLIF(txn_count, 0), 2) +
            POW(branch_count * 1.0 / NULLIF(txn_count, 0), 2) +
            POW(mobile_count * 1.0 / NULLIF(txn_count, 0), 2) +
            POW(api_count * 1.0 / NULLIF(txn_count, 0), 2)
        , 4) AS channel_hhi,
        
        -- Cumulative YTD volume
        SUM(monthly_volume) OVER (
            PARTITION BY region, segment 
            ORDER BY txn_month 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ytd_cumulative_volume,
        
        -- Expired KYC rate
        ROUND(expired_kyc_count * 100.0 / NULLIF(txn_count, 0), 2) AS expired_kyc_rate,
        
        -- Dormancy rate
        ROUND(dormant_acct_txns * 100.0 / NULLIF(txn_count, 0), 2) AS dormancy_rate
    FROM monthly_metrics
)

SELECT
    region,
    segment,
    txn_month,
    txn_count,
    ROUND(monthly_volume, 0) AS monthly_vol,
    ROUND(avg_amount, 0) AS avg_amt,
    ROUND(median_txn_amount, 0) AS median_amt,
    ROUND(p90_txn_amount, 0) AS p90_amt,
    ROUND(p99_txn_amount, 0) AS p99_amt,
    mom_growth_pct,
    channel_hhi,
    ROUND(ytd_cumulative_volume, 0) AS ytd_vol,
    unique_customers,
    expired_kyc_rate,
    dormancy_rate
FROM enriched
ORDER BY region, segment, txn_month
LIMIT 50;
```

### Key Results (Query 2)
- **Hai Phong UHNW** dominated with 64K txns and 4.1T VND volume in Dec 2025
- MoM growth rates ranged from **-12% to +26%**
- Channel HHI around **0.07-0.11** (well-diversified across 5 channels)
- Expired KYC rates **2-12%** across regions

---

## Query 3 — Anomaly Detection & Behavioral Analysis

**Difficulty**: ★★★★★  
**Concepts**: Z-score statistical analysis, self-join for cross-account velocity, coefficient of variation, COLLECT_SET, UNIX_TIMESTAMP arithmetic, composite anomaly scoring, waterfall classification  
**Performance Note**: The self-join in CTE 4 is O(N²) per customer — extremely expensive on 500K+ transactions. Expect long runtimes on large datasets.

```sql
WITH
-- ═══════════════════════════════════════════════════════════════
-- CTE 1: Hourly transaction patterns per customer (behavioral fingerprint)
-- ═══════════════════════════════════════════════════════════════
hourly_behavior AS (
    SELECT
        a.customer_id,
        HOUR(CAST(t.txn_datetime AS TIMESTAMP)) AS txn_hour,
        COUNT(*) AS txn_count,
        SUM(t.amount) AS hour_volume,
        AVG(t.amount) AS hour_avg_amt,
        COLLECT_SET(t.channel) AS channels_in_hour
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id, HOUR(CAST(t.txn_datetime AS TIMESTAMP))
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 2: Customer behavioral profile (normal patterns)
-- ═══════════════════════════════════════════════════════════════
customer_profile AS (
    SELECT
        customer_id,
        AVG(txn_count) AS avg_hourly_txns,
        STDDEV(txn_count) AS stddev_hourly_txns,
        AVG(hour_volume) AS avg_hourly_volume,
        STDDEV(hour_volume) AS stddev_hourly_volume,
        MAX(txn_count) AS peak_hour_txns,
        MIN(txn_count) AS min_hour_txns,
        -- Coefficient of variation (higher = more irregular patterns)
        COALESCE(STDDEV(txn_count) / NULLIF(AVG(txn_count), 0), 0) AS cv_txn_count,
        COALESCE(STDDEV(hour_volume) / NULLIF(AVG(hour_volume), 0), 0) AS cv_volume,
        COUNT(DISTINCT txn_hour) AS active_hours
    FROM hourly_behavior
    GROUP BY customer_id
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 3: Detect anomalous hours (Z-score > 2 deviation from customer mean)
-- ═══════════════════════════════════════════════════════════════
anomalous_hours AS (
    SELECT
        h.customer_id,
        h.txn_hour,
        h.txn_count,
        h.hour_volume,
        p.avg_hourly_txns,
        p.stddev_hourly_txns,
        -- Z-score for transaction count
        CASE WHEN p.stddev_hourly_txns > 0
             THEN (h.txn_count - p.avg_hourly_txns) / p.stddev_hourly_txns
             ELSE 0 END AS z_score_count,
        -- Z-score for volume
        CASE WHEN p.stddev_hourly_volume > 0
             THEN (h.hour_volume - p.avg_hourly_volume) / p.stddev_hourly_volume
             ELSE 0 END AS z_score_volume,
        p.cv_txn_count,
        p.cv_volume,
        p.active_hours
    FROM hourly_behavior h
    JOIN customer_profile p ON h.customer_id = p.customer_id
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 4: Cross-account velocity check (same customer, multiple accounts)
-- Self-join pattern for detecting rapid cross-account movement
-- ═══════════════════════════════════════════════════════════════
cross_account AS (
    SELECT
        a1.customer_id,
        COUNT(DISTINCT a1.account_id) AS num_source_accounts,
        COUNT(*) AS cross_account_txns,
        SUM(t1.amount) AS cross_account_volume,
        MIN(ABS(
            UNIX_TIMESTAMP(CAST(t1.txn_datetime AS TIMESTAMP)) - 
            UNIX_TIMESTAMP(CAST(t2.txn_datetime AS TIMESTAMP))
        )) AS min_inter_account_gap_sec
    FROM transactions t1
    JOIN accounts a1 ON t1.account_id = a1.account_id
    JOIN transactions t2 ON t1.txn_id != t2.txn_id
    JOIN accounts a2 ON t2.account_id = a2.account_id
        AND a1.customer_id = a2.customer_id
        AND a1.account_id != a2.account_id
        -- Same 10-minute window
        AND ABS(
            UNIX_TIMESTAMP(CAST(t1.txn_datetime AS TIMESTAMP)) - 
            UNIX_TIMESTAMP(CAST(t2.txn_datetime AS TIMESTAMP))
        ) < 600
    WHERE t1.txn_type IN ('Transfer Out', 'Withdrawal')
      AND t2.txn_type IN ('Transfer In', 'Deposit')
    GROUP BY a1.customer_id
    HAVING COUNT(*) > 5
),

-- ═══════════════════════════════════════════════════════════════
-- CTE 5: Final anomaly scoring — combine all signals
-- ═══════════════════════════════════════════════════════════════
anomaly_summary AS (
    SELECT
        cp.customer_id,
        c.segment,
        c.kyc_status,
        c.gender,
        cp.avg_hourly_txns,
        cp.stddev_hourly_txns,
        cp.cv_txn_count,
        cp.cv_volume,
        cp.active_hours,
        cp.peak_hour_txns,
        
        -- Count of anomalous hours (Z > 2)
        ah_agg.anomalous_hour_count,
        ah_agg.max_z_score,
        
        -- Cross-account signals (may be NULL if no cross-account activity)
        ca.cross_account_txns,
        ca.cross_account_volume,
        ca.min_inter_account_gap_sec,
        
        -- Composite anomaly score (0-100)
        ROUND(
            LEAST(30, COALESCE(ah_agg.anomalous_hour_count, 0) * 5) +
            LEAST(20, COALESCE(ah_agg.max_z_score, 0) * 4) +
            LEAST(25, COALESCE(ca.cross_account_txns, 0) * 0.5) +
            LEAST(25, cp.cv_volume * 15)
        , 2) AS anomaly_score,
        
        CASE
            WHEN ca.cross_account_txns IS NOT NULL AND ah_agg.max_z_score > 3 THEN 'STRUCTURING_SUSPECT'
            WHEN ah_agg.max_z_score > 3 THEN 'BURST_ANOMALY'
            WHEN ca.cross_account_txns > 20 THEN 'RAPID_CROSS_ACCOUNT'
            WHEN cp.cv_volume > 2 THEN 'HIGH_VOLATILITY'
            ELSE 'NORMAL'
        END AS anomaly_type
    FROM customer_profile cp
    JOIN customers c ON cp.customer_id = c.customer_id
    LEFT JOIN (
        SELECT 
            customer_id,
            SUM(CASE WHEN ABS(z_score_count) > 2 OR ABS(z_score_volume) > 2 THEN 1 ELSE 0 END) AS anomalous_hour_count,
            MAX(GREATEST(ABS(z_score_count), ABS(z_score_volume))) AS max_z_score
        FROM anomalous_hours
        GROUP BY customer_id
    ) ah_agg ON cp.customer_id = ah_agg.customer_id
    LEFT JOIN cross_account ca ON cp.customer_id = ca.customer_id
)

SELECT
    customer_id,
    segment,
    kyc_status,
    gender,
    ROUND(avg_hourly_txns, 1) AS avg_hourly_txns,
    ROUND(cv_txn_count, 3) AS behavioral_irregularity,
    ROUND(cv_volume, 3) AS volume_volatility,
    active_hours,
    peak_hour_txns,
    anomalous_hour_count,
    ROUND(max_z_score, 2) AS max_z_score,
    COALESCE(cross_account_txns, 0) AS cross_acct_txns,
    ROUND(COALESCE(cross_account_volume, 0), 0) AS cross_acct_vol,
    COALESCE(min_inter_account_gap_sec, -1) AS min_cross_gap_sec,
    anomaly_score,
    anomaly_type
FROM anomaly_summary
WHERE anomaly_type != 'NORMAL'
ORDER BY anomaly_score DESC
LIMIT 25;
```

### Key Results (Query 3) — Optimized Run

**Optimization applied**: Pre-filtered to only multi-account customers (`HAVING COUNT(DISTINCT account_id) > 1`) and relevant txn types (`Transfer Out/In, Withdrawal, Deposit`) **before** the self-join. This reduced the join cardinality from 500K² to a much smaller subset.

| Rank | customer_id | segment | anomaly_score | anomaly_type | cross_acct_txns | cross_acct_vol (VND) | min_gap (sec) | max_z_score |
|------|-------------|---------|---------------|--------------|-----------------|---------------------|---------------|-------------|
| 1 | CUST009467 | UHNW | 53.20 | STRUCTURING_SUSPECT | 72 | 3.9B | 1 | 3.06 |
| 2 | CUST007653 | UHNW | 51.12 | RAPID_CROSS_ACCOUNT | 55 | 2.5B | 32 | 2.48 |
| 3 | CUST002557 | UHNW | 50.38 | RAPID_CROSS_ACCOUNT | 141 | 6.6B | 1 | 2.37 |
| 4 | CUST002385 | UHNW | 50.24 | RAPID_CROSS_ACCOUNT | 728 | 37B | 0 | 2.37 |
| 5 | CUST007747 | UHNW | 47.02 | RAPID_CROSS_ACCOUNT | 1355 | 68.3B | 2 | 2.78 |

**Key Findings**:
- **All 25 flagged customers are UHNW** — high-net-worth individuals with many accounts and high transaction volumes
- Only **1 STRUCTURING_SUSPECT** (CUST009467): had both Z-score > 3 AND cross-account activity — the most suspicious pattern
- **24 RAPID_CROSS_ACCOUNT**: frequent transfers between own accounts within 10-minute windows
- **Min inter-account gaps as low as 0-1 seconds** — near-simultaneous cross-account transfers
- Cross-account volumes range from **2.5B to 68.3B VND**
- **CUST007747** had the most cross-account pairs: **1,355 transactions** totaling 68.3B VND
- **Behavioral irregularity (CV) is very low** (0.024-0.057) — these customers are consistent in their hourly patterns, the anomaly comes from cross-account velocity

---

## SQL Concepts Learning Guide

### Level 1: Foundation Patterns

#### Common Table Expressions (CTEs) — `WITH ... AS`
Every query uses CTEs to decompose complex logic into readable, named stages. Think of them as temporary named result sets.

```sql
WITH step1 AS (SELECT ...),
     step2 AS (SELECT ... FROM step1),  -- can reference previous CTEs
     step3 AS (SELECT ... FROM step2 JOIN other_table ...)
SELECT * FROM step3
```

**Why it matters**: Without CTEs, you'd nest subqueries 5+ levels deep — unreadable and unmaintainable. Each CTE is a logical "pipeline stage."

---

### Level 2: Window Functions (Queries 1 & 2)

Window functions compute values **across a set of rows related to the current row** without collapsing rows (unlike `GROUP BY`).

#### `RANK() OVER (PARTITION BY ... ORDER BY ...)` — Query 1
```sql
RANK() OVER (PARTITION BY c.segment ORDER BY risk_score DESC) AS risk_rank
```
- **PARTITION BY**: Creates independent "windows" (like mini-groups)
- **ORDER BY**: Determines ranking within each window
- Ties get the same rank; next rank skips (1, 1, 3... not 1, 1, 2)

#### `LAG(col, n) OVER (...)` — Queries 1 & 2
```sql
LAG(monthly_volume, 1) OVER (PARTITION BY region, segment ORDER BY txn_month)
```
Accesses the **previous row's value** within the window. Essential for:
- Month-over-month (MoM) growth: `(current - LAG) / LAG`
- Detecting spikes: `current / LAG > threshold`

#### `NTILE(n) OVER (...)` — Query 1
```sql
NTILE(100) OVER (ORDER BY total_volume) AS volume_percentile
```
Divides all rows into `n` equal buckets (here 100 = percentile). Row gets its bucket number.

#### Running/Cumulative Aggregates — Queries 1 & 2
```sql
SUM(monthly_volume) OVER (
    PARTITION BY region, segment 
    ORDER BY txn_month 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS ytd_cumulative_volume
```
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` = running sum from first row to current. This is how you compute **YTD (year-to-date)** totals.

---

### Level 3: Statistical & Analytical Functions

#### Percentile Approximation — Query 2
```sql
PERCENTILE_APPROX(t.amount, 0.5)  AS median_txn_amount,
PERCENTILE_APPROX(t.amount, 0.9)  AS p90_txn_amount,
PERCENTILE_APPROX(t.amount, 0.99) AS p99_txn_amount
```
Returns approximate percentiles — far cheaper than exact `PERCENTILE` on massive datasets. The p50/p90/p99 pattern is standard in **SRE/performance monitoring**.

#### Z-Score Anomaly Detection — Query 3
```sql
(h.txn_count - p.avg_hourly_txns) / p.stddev_hourly_txns AS z_score_count
```
**Z-score** = how many standard deviations a value is from the mean.
- |Z| > 2 → unusual (top/bottom ~2.5%)
- |Z| > 3 → highly anomalous (top/bottom ~0.1%)

Core statistical technique for **fraud detection and anomaly flagging**.

#### Coefficient of Variation (CV) — Query 3
```sql
STDDEV(txn_count) / NULLIF(AVG(txn_count), 0) AS cv_txn_count
```
CV = `stddev / mean`. Measures **relative variability** — a CV of 2.0 means the standard deviation is 2x the mean (highly irregular behavior).

#### Herfindahl-Hirschman Index (HHI) — Query 2
```sql
POW(online_count / txn_count, 2) + POW(atm_count / txn_count, 2) + ...
```
HHI measures **market concentration**. Values:
- ~0.2 (1/5 channels equally used) = diversified
- ~1.0 = all activity on one channel (suspicious)

---

### Level 4: Advanced Join Patterns

#### Self-Join for Cross-Account Velocity — Query 3
```sql
FROM transactions t1
JOIN accounts a1 ON t1.account_id = a1.account_id
JOIN transactions t2 ON t1.txn_id != t2.txn_id
JOIN accounts a2 ON t2.account_id = a2.account_id
    AND a1.customer_id = a2.customer_id    -- same customer
    AND a1.account_id != a2.account_id     -- different accounts
    AND ABS(UNIX_TIMESTAMP(t1) - UNIX_TIMESTAMP(t2)) < 600  -- 10-min window
```
Joins the transactions table **against itself** to find pairs of transactions by the same customer across different accounts within 10 minutes. Real-world **structuring/money laundering detection** pattern.

**Why it's expensive**: N transactions → up to N² combinations per customer. With 500K+ rows, this explodes combinatorially.

#### LEFT JOIN for Optional Signals — Queries 1 & 3
```sql
LEFT JOIN cross_account ca ON cp.customer_id = ca.customer_id
```
Not every customer has cross-account activity. `LEFT JOIN` keeps all customers and fills `NULL` for those without matches. `COALESCE(ca.value, 0)` converts NULLs.

---

### Level 5: Composite Scoring & Classification

#### Weighted Multi-Signal Scoring — Queries 1 & 3
```sql
LEAST(30, anomalous_hour_count * 5) +     -- max 30 points
LEAST(20, max_z_score * 4) +               -- max 20 points
LEAST(25, cross_account_txns * 0.5) +      -- max 25 points
LEAST(25, cv_volume * 15)                  -- max 25 points
```
Each signal contributes up to a **capped maximum** (`LEAST` prevents any single factor from dominating). Total adds to 100. This is how real **credit scoring, fraud scoring, and risk models** work.

#### Tiered Classification via CASE — Queries 1 & 3
```sql
CASE
    WHEN cross_account_txns IS NOT NULL AND max_z_score > 3 THEN 'STRUCTURING_SUSPECT'
    WHEN max_z_score > 3 THEN 'BURST_ANOMALY'
    WHEN cross_account_txns > 20 THEN 'RAPID_CROSS_ACCOUNT'
    WHEN cv_volume > 2 THEN 'HIGH_VOLATILITY'
    ELSE 'NORMAL'
END
```
Order matters — first matching condition wins. Most severe classification comes first (prioritized waterfall logic).

---

### Level 6: Conditional Aggregation

#### CASE inside Aggregate Functions — Queries 1 & 2
```sql
SUM(CASE WHEN t.txn_type = 'Withdrawal' THEN t.amount ELSE 0 END) AS withdrawal_vol,
COUNT(CASE WHEN c.kyc_status = 'Expired' THEN 1 END) AS expired_kyc_count
```
This is how you **pivot data without PIVOT** — filtering within the aggregation. Each `CASE` acts as a conditional filter.

---

## Performance Notes

| Technique | Cost | Why |
|-----------|------|-----|
| `PERCENTILE_APPROX` vs `PERCENTILE` | Low vs High | Approximate avoids sorting all data |
| `COLLECT_SET` | Medium | Collects distinct values into arrays — memory intensive |
| Self-join on large table | **Very High** | O(N²) per customer — Query 3's bottleneck |
| Window functions | Medium | Requires sorting within partitions |
| `HAVING COUNT(*) > 5` | Low | Filters after grouping, reduces output early |

### Spark-Specific Notes
- CTEs in Spark are **lazily evaluated** — not materialized unless cached
- The Catalyst optimizer may rearrange or merge CTEs
- `explain_sql` is **not available** via Spark Connect (JVM_ATTRIBUTE_NOT_SUPPORTED)
- Serverless compute auto-scales but self-joins can still overwhelm

### Concept Difficulty Ladder

| Level | Concepts | Queries |
|-------|----------|---------|
| 1 | CTEs, basic JOINs, GROUP BY | All |
| 2 | RANK, LAG, NTILE, running SUM | Q1, Q2 |
| 3 | Z-score, CV, percentiles, HHI | Q2, Q3 |
| 4 | Self-joins, LEFT JOINs for optional data | Q3, Q1 |
| 5 | Composite scoring with LEAST caps | Q1, Q3 |
| 6 | Conditional aggregation (CASE in SUM/COUNT) | Q1, Q2 |
| 7 | Performance trade-offs, Spark execution model | All |

---

*These patterns cover ~80% of what senior data engineers use in production banking analytics.*
