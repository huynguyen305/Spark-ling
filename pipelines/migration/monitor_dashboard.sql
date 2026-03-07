--------------------------------------------------------------------------------
-- Databricks SQL Dashboard Queries for Migration Monitoring
-- ==========================================================
-- Create a Databricks SQL Dashboard using these queries to monitor
-- the health and progress of your RDS → Databricks migration.
--
-- LEARNING NOTES:
-- ---------------
-- 1. Databricks SQL Dashboards are built from saved SQL queries.
-- 2. Each query below becomes a "widget" on the dashboard.
-- 3. In production, dashboards refresh automatically on a schedule.
-- 4. At Techcombank, similar dashboards track daily pipeline health.
--
-- HOW TO CREATE THE DASHBOARD:
-- 1. Go to Databricks workspace → SQL → Queries
-- 2. Create each query below as a saved query
-- 3. Go to SQL → Dashboards → Create Dashboard
-- 4. Add each saved query as a widget
-- 5. Set auto-refresh interval (e.g., every 15 minutes)
--
-- PREREQUISITES:
-- Tables must exist in the sparkling.banking schema.
-- If using external S3 Delta tables, register them first:
--   CREATE TABLE sparkling.banking.bronze_customers
--   USING DELTA LOCATION 's3://sparkling-data-test/migration/bronze/dim_customer';
--------------------------------------------------------------------------------


-- ============================================================================
-- QUERY 1: Pipeline Layer Row Counts
-- ====================================
-- Shows row counts at each layer (Bronze → Silver → Gold).
-- LEARNING: If Bronze > Silver, rows were quarantined for quality issues.
-- If Silver ≠ Gold, the aggregation reduced rows (expected).
-- ============================================================================
-- Widget type: Table / Counter
-- Title: "Data Layer Health Summary"

SELECT 'Bronze - Customers' AS layer,
       COUNT(*) AS row_count,
       MAX(_ingestion_timestamp) AS latest_refresh
FROM sparkling.banking.bronze_customers
UNION ALL
SELECT 'Bronze - Transactions',
       COUNT(*),
       MAX(_ingestion_timestamp)
FROM sparkling.banking.bronze_transactions
UNION ALL
SELECT 'Silver - Customers',
       COUNT(*),
       MAX(_silver_timestamp)
FROM sparkling.banking.silver_customers
UNION ALL
SELECT 'Silver - Transactions',
       COUNT(*),
       MAX(_silver_timestamp)
FROM sparkling.banking.silver_transactions
UNION ALL
SELECT 'Gold - Customer 360',
       COUNT(*),
       MAX(_gold_timestamp)
FROM sparkling.banking.gold_customer_360
UNION ALL
SELECT 'Gold - Branch Summary',
       COUNT(*),
       MAX(_gold_timestamp)
FROM sparkling.banking.gold_daily_branch_summary
ORDER BY layer;


-- ============================================================================
-- QUERY 2: Daily Transaction Volume Trend
-- =========================================
-- Line chart showing transaction volume over time.
-- LEARNING: This is the most important operational metric.
-- A sudden drop could indicate pipeline failure or source issues.
-- ============================================================================
-- Widget type: Line Chart (X=txn_date, Y=daily_txn_count)
-- Title: "Daily Transaction Volume"

SELECT CAST(SUBSTRING(CAST(txn_date_key AS STRING), 1, 4) || '-' ||
            SUBSTRING(CAST(txn_date_key AS STRING), 5, 2) || '-' ||
            SUBSTRING(CAST(txn_date_key AS STRING), 7, 2) AS DATE) AS txn_date,
       COUNT(*) AS daily_txn_count,
       SUM(amount) AS daily_total_amount,
       AVG(amount) AS daily_avg_amount,
       COUNT(DISTINCT customer_id) AS unique_customers
FROM sparkling.banking.silver_transactions
GROUP BY txn_date_key
ORDER BY txn_date DESC
LIMIT 90;


-- ============================================================================
-- QUERY 3: Regional Transaction Heatmap
-- =======================================
-- Transaction volume by region — identifies high-activity areas.
-- LEARNING: At Techcombank, Ho Chi Minh and Hanoi dominate (~70% of volume).
-- ============================================================================
-- Widget type: Bar Chart (X=region, Y=total_amount)
-- Title: "Transaction Volume by Region"

SELECT region,
       COUNT(*) AS txn_count,
       SUM(total_amount) AS total_amount,
       AVG(total_amount) AS avg_daily_amount,
       SUM(suspicious_count) AS total_suspicious
FROM sparkling.banking.gold_daily_branch_summary
GROUP BY region
ORDER BY total_amount DESC;


-- ============================================================================
-- QUERY 4: Customer Segment Distribution
-- ========================================
-- Pie chart of customer segments.
-- LEARNING: Segment distribution affects revenue forecasting.
-- VIP customers (~5%) typically generate ~40% of transaction value.
-- ============================================================================
-- Widget type: Pie Chart (Labels=segment, Values=customer_count)
-- Title: "Customer Segment Distribution"

SELECT segment,
       COUNT(*) AS customer_count,
       ROUND(AVG(total_transactions), 0) AS avg_transactions,
       ROUND(AVG(annualized_value), 2) AS avg_annualized_value,
       ROUND(AVG(risk_score), 1) AS avg_risk_score
FROM sparkling.banking.gold_customer_360
GROUP BY segment
ORDER BY customer_count DESC;


-- ============================================================================
-- QUERY 5: Data Freshness Monitor
-- =================================
-- Shows how recently each table was updated.
-- LEARNING: Stale data is a sign of pipeline failure. Set alerts
-- if data is older than expected (e.g., > 2 hours for daily pipeline).
-- ============================================================================
-- Widget type: Table with conditional formatting
-- Title: "Data Freshness (Last Update)"

SELECT 'Bronze Customers' AS table_name,
       MAX(_ingestion_timestamp) AS last_updated,
       TIMESTAMPDIFF(HOUR, MAX(_ingestion_timestamp), CURRENT_TIMESTAMP()) AS hours_stale
FROM sparkling.banking.bronze_customers
UNION ALL
SELECT 'Bronze Transactions',
       MAX(_ingestion_timestamp),
       TIMESTAMPDIFF(HOUR, MAX(_ingestion_timestamp), CURRENT_TIMESTAMP())
FROM sparkling.banking.bronze_transactions
UNION ALL
SELECT 'Gold Customer 360',
       MAX(_gold_timestamp),
       TIMESTAMPDIFF(HOUR, MAX(_gold_timestamp), CURRENT_TIMESTAMP())
FROM sparkling.banking.gold_customer_360
ORDER BY hours_stale DESC;


-- ============================================================================
-- QUERY 6: Risk Alerts Summary
-- ==============================
-- Count of suspicious transactions by risk level and type.
-- LEARNING: This is a simplified AML (Anti-Money Laundering) dashboard.
-- Real banks have dedicated compliance systems (NICE Actimize, etc.).
-- ============================================================================
-- Widget type: Bar Chart (X=risk_level, Y=alert_count)
-- Title: "Risk Alerts by Level"

SELECT risk_level,
       suspicious_flag,
       COUNT(*) AS alert_count,
       SUM(amount) AS total_flagged_amount,
       COUNT(DISTINCT customer_id) AS affected_customers
FROM sparkling.banking.gold_risk_alerts
GROUP BY risk_level, suspicious_flag
ORDER BY
    CASE risk_level
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        ELSE 4
    END;


-- ============================================================================
-- QUERY 7: Top 10 Customers by Transaction Value
-- =================================================
-- Identifies highest-value customers.
-- LEARNING: This feeds into CRM systems for relationship management.
-- ============================================================================
-- Widget type: Table
-- Title: "Top 10 Customers by Value"

SELECT customer_id,
       full_name,
       segment,
       region,
       total_transactions,
       total_amount,
       annualized_value,
       preferred_channel,
       risk_score,
       customer_tenure_days
FROM sparkling.banking.gold_customer_360
WHERE total_transactions IS NOT NULL
ORDER BY total_amount DESC
LIMIT 10;


-- ============================================================================
-- QUERY 8: Channel Usage Distribution
-- =====================================
-- Shows which channels customers prefer.
-- LEARNING: Digital transformation is measured by channel shift.
-- Banks track the % of transactions moving from Branch → Mobile App.
-- ============================================================================
-- Widget type: Pie Chart or Bar Chart
-- Title: "Transaction Channel Distribution"

SELECT channel,
       COUNT(*) AS txn_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
       SUM(amount) AS total_amount,
       is_business_hours
FROM sparkling.banking.silver_transactions
GROUP BY channel, is_business_hours
ORDER BY txn_count DESC;
