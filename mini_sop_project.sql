/* =====================================================================
   Mini S&OP Data Model in Snowflake - full project script
   Order of execution:
     0-1  setup + raw tables
          -> run generate_raw_data.sql here (fills the raw tables)
     2    staging
     3    data quality checks
     4    quarantine + valid views
     5    mart (dimensions + facts) + reconciliation
     6    S&OP views
     7    extra Snowflake features
   Use "Run all" for each section.
   ===================================================================== */


/* ---------------------------------------------------------------------
   0. Setup
   --------------------------------------------------------------------- */
CREATE WAREHOUSE IF NOT EXISTS sop_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS sop_db;
CREATE SCHEMA IF NOT EXISTS sop_db.raw;
CREATE SCHEMA IF NOT EXISTS sop_db.staging;
CREATE SCHEMA IF NOT EXISTS sop_db.mart;

USE WAREHOUSE sop_wh;
USE DATABASE sop_db;


/* ---------------------------------------------------------------------
   1. RAW layer (all VARCHAR, nothing rejected)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE raw.sales_raw (
  order_id VARCHAR, order_date VARCHAR, product_code VARCHAR,
  product_name VARCHAR, category VARCHAR, region VARCHAR,
  quantity VARCHAR, sales_amount VARCHAR,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.inventory_raw (
  product_code VARCHAR, warehouse VARCHAR,
  snapshot_date VARCHAR, stock_qty VARCHAR,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- >>> Now run generate_raw_data.sql to fill the raw tables <<<


/* ---------------------------------------------------------------------
   2. STAGING layer: typing, standardizing, de-duplication
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE staging.sales_clean AS
WITH cleaned AS (
  SELECT
    TRIM(order_id)                                                AS order_id,
    TRY_TO_DATE(order_date)                                       AS order_date,
    UPPER(REGEXP_REPLACE(TRIM(product_code), '[^A-Za-z0-9]', '')) AS product_code,
    INITCAP(TRIM(product_name))                                   AS product_name,
    INITCAP(TRIM(category))                                       AS category,
    UPPER(TRIM(region))                                           AS region,
    TRY_TO_NUMBER(quantity)                                       AS quantity,
    TRY_TO_DECIMAL(sales_amount, 18, 2)                           AS sales_amount,
    load_ts
  FROM raw.sales_raw
  WHERE order_id IS NOT NULL
)
SELECT order_id, order_date, product_code, product_name,
       category, region, quantity, sales_amount
FROM cleaned
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY order_id, product_code
          ORDER BY load_ts DESC
        ) = 1;

CREATE OR REPLACE TABLE staging.inventory_clean AS
WITH cleaned AS (
  SELECT
    UPPER(REGEXP_REPLACE(TRIM(product_code), '[^A-Za-z0-9]', '')) AS product_code,
    UPPER(TRIM(warehouse))                                        AS warehouse,
    TRY_TO_DATE(snapshot_date)                                    AS snapshot_date,
    TRY_TO_NUMBER(stock_qty)                                      AS stock_qty,
    load_ts
  FROM raw.inventory_raw
  WHERE product_code IS NOT NULL
)
SELECT product_code, warehouse, snapshot_date, stock_qty
FROM cleaned
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY product_code, warehouse, snapshot_date
          ORDER BY load_ts DESC
        ) = 1;


/* ---------------------------------------------------------------------
   3. Data quality checks
   --------------------------------------------------------------------- */
CREATE OR REPLACE VIEW staging.v_dq_checks AS
WITH checks AS (
  SELECT 'sales: duplicates removed' AS check_name,
         (SELECT COUNT(*) FROM raw.sales_raw) - (SELECT COUNT(*) FROM staging.sales_clean) AS failing_rows,
         'Raw rows minus clean rows (duplicates / null order_id)' AS description
  UNION ALL
  SELECT 'sales: invalid date', COUNT(*), 'order_date could not be parsed'
  FROM staging.sales_clean WHERE order_date IS NULL
  UNION ALL
  SELECT 'sales: date out of range', COUNT(*), 'order_date outside 2025-01-01 .. 2026-09-30'
  FROM staging.sales_clean
  WHERE order_date IS NOT NULL
    AND (order_date < '2025-01-01' OR order_date > '2026-09-30')
  UNION ALL
  SELECT 'sales: quantity <= 0', COUNT(*), 'Negative or zero quantity'
  FROM staging.sales_clean WHERE quantity <= 0
  UNION ALL
  SELECT 'sales: negative amount', COUNT(*), 'sales_amount < 0'
  FROM staging.sales_clean WHERE sales_amount < 0
  UNION ALL
  SELECT 'sales: missing product code', COUNT(*), 'product_code is null or empty'
  FROM staging.sales_clean WHERE product_code IS NULL OR product_code = ''
  UNION ALL
  SELECT 'inventory: duplicates removed',
         (SELECT COUNT(*) FROM raw.inventory_raw) - (SELECT COUNT(*) FROM staging.inventory_clean),
         'Raw rows minus clean rows'
  UNION ALL
  SELECT 'inventory: negative stock', COUNT(*), 'stock_qty < 0'
  FROM staging.inventory_clean WHERE stock_qty < 0
  UNION ALL
  SELECT 'inventory: product not in sales', COUNT(DISTINCT i.product_code), 'Orphan keys (no match in sales)'
  FROM staging.inventory_clean i
  LEFT JOIN (SELECT DISTINCT product_code FROM staging.sales_clean) s
         ON i.product_code = s.product_code
  WHERE s.product_code IS NULL
  UNION ALL
  SELECT 'sales: product not in inventory', COUNT(DISTINCT s.product_code), 'Orphan keys (no match in inventory)'
  FROM staging.sales_clean s
  LEFT JOIN (SELECT DISTINCT product_code FROM staging.inventory_clean) i
         ON s.product_code = i.product_code
  WHERE s.product_code IS NOT NULL AND i.product_code IS NULL
)
SELECT check_name,
       failing_rows,
       CASE WHEN failing_rows = 0 THEN 'PASS' ELSE 'REVIEW' END AS status,
       description
FROM checks;

SELECT * FROM staging.v_dq_checks ORDER BY status DESC, check_name;


/* ---------------------------------------------------------------------
   4. Quarantine tables and valid-row views
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE staging.sales_rejected AS
SELECT
  s.*,
  ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
    IFF(order_date IS NULL, 'invalid_date', NULL),
    IFF(product_code IS NULL OR product_code = '', 'missing_product_code', NULL),
    IFF(quantity IS NULL OR quantity <= 0, 'invalid_quantity', NULL),
    IFF(sales_amount IS NULL OR sales_amount < 0, 'invalid_amount', NULL)
  ), ', ')                 AS reject_reason,
  CURRENT_TIMESTAMP()      AS rejected_ts
FROM staging.sales_clean s
WHERE order_date IS NULL
   OR product_code IS NULL OR product_code = ''
   OR quantity IS NULL OR quantity <= 0
   OR sales_amount IS NULL OR sales_amount < 0;

CREATE OR REPLACE TABLE staging.inventory_rejected AS
SELECT
  i.*,
  ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
    IFF(snapshot_date IS NULL, 'invalid_date', NULL),
    IFF(stock_qty IS NULL OR stock_qty < 0, 'invalid_stock', NULL)
  ), ', ')                 AS reject_reason,
  CURRENT_TIMESTAMP()      AS rejected_ts
FROM staging.inventory_clean i
WHERE snapshot_date IS NULL
   OR stock_qty IS NULL OR stock_qty < 0;

CREATE OR REPLACE VIEW staging.sales_valid AS
SELECT * FROM staging.sales_clean
WHERE order_date IS NOT NULL
  AND product_code IS NOT NULL AND product_code <> ''
  AND quantity > 0
  AND sales_amount >= 0;

CREATE OR REPLACE VIEW staging.inventory_valid AS
SELECT * FROM staging.inventory_clean
WHERE snapshot_date IS NOT NULL
  AND stock_qty >= 0;


/* ---------------------------------------------------------------------
   5. MART layer: common dimensions and facts
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE mart.dim_product AS
WITH all_products AS (
  SELECT product_code, product_name, category FROM staging.sales_valid
  UNION
  SELECT product_code, NULL, NULL FROM staging.inventory_valid
)
SELECT
  ROW_NUMBER() OVER (ORDER BY product_code)  AS product_key,
  product_code,
  COALESCE(MAX(product_name), 'UNKNOWN')     AS product_name,
  COALESCE(MAX(category), 'UNKNOWN')         AS category
FROM all_products
GROUP BY product_code;

CREATE OR REPLACE TABLE mart.dim_date AS
SELECT
  d                       AS date_key,
  YEAR(d)                 AS year,
  QUARTER(d)              AS quarter,
  MONTH(d)                AS month,
  TO_CHAR(d, 'YYYY-MM')   AS year_month,
  DATE_TRUNC('month', d)  AS month_start,
  WEEKOFYEAR(d)           AS week_of_year,
  DAYNAME(d)              AS day_name
FROM (
  SELECT DATEADD(day, SEQ4(), '2020-01-01'::DATE) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 3650))
);

CREATE OR REPLACE TABLE mart.fact_sales AS
SELECT
  s.order_id,
  s.order_date     AS date_key,
  p.product_key,
  s.region,
  s.quantity,
  s.sales_amount
FROM staging.sales_valid s
JOIN mart.dim_product p ON s.product_code = p.product_code;

CREATE OR REPLACE TABLE mart.fact_inventory AS
SELECT
  i.snapshot_date  AS date_key,
  p.product_key,
  i.warehouse,
  i.stock_qty
FROM staging.inventory_valid i
JOIN mart.dim_product p ON i.product_code = p.product_code;

-- Reconciliation: unexplained_difference must be 0
SELECT
  (SELECT COUNT(*) FROM staging.sales_clean)    AS clean_rows,
  (SELECT COUNT(*) FROM mart.fact_sales)        AS loaded_to_fact,
  (SELECT COUNT(*) FROM staging.sales_rejected) AS rejected,
  (SELECT COUNT(*) FROM staging.sales_clean)
    - (SELECT COUNT(*) FROM mart.fact_sales)
    - (SELECT COUNT(*) FROM staging.sales_rejected) AS unexplained_difference;

-- Key integrity: must be 0
SELECT COUNT(*) AS facts_without_date
FROM mart.fact_sales f
LEFT JOIN mart.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;


/* ---------------------------------------------------------------------
   6. S&OP views
   --------------------------------------------------------------------- */
CREATE OR REPLACE VIEW mart.v_monthly_sales AS
SELECT
  d.year_month,
  d.month_start,
  p.product_code,
  p.product_name,
  p.category,
  SUM(f.quantity)      AS total_qty,
  SUM(f.sales_amount)  AS total_sales
FROM mart.fact_sales f
JOIN mart.dim_date    d ON f.date_key    = d.date_key
JOIN mart.dim_product p ON f.product_key = p.product_key
GROUP BY d.year_month, d.month_start, p.product_code, p.product_name, p.category;

CREATE OR REPLACE VIEW mart.v_stock_coverage AS
WITH latest_stock AS (
  SELECT product_key, SUM(stock_qty) AS stock_qty
  FROM mart.fact_inventory
  WHERE date_key = (SELECT MAX(date_key) FROM mart.fact_inventory)
  GROUP BY product_key
),
demand AS (
  SELECT product_key, SUM(quantity) / 3.0 AS avg_monthly_demand
  FROM mart.fact_sales
  WHERE date_key >= DATEADD(month, -3, DATE_TRUNC('month', CURRENT_DATE()))
    AND date_key <  DATE_TRUNC('month', CURRENT_DATE())
  GROUP BY product_key
)
SELECT
  p.product_code,
  p.product_name,
  s.stock_qty,
  ROUND(d.avg_monthly_demand, 1)                          AS avg_monthly_demand,
  ROUND(s.stock_qty / NULLIF(d.avg_monthly_demand, 0), 1) AS months_of_cover,
  CASE
    WHEN d.avg_monthly_demand IS NULL THEN 'NO DEMAND DATA'
    WHEN s.stock_qty / NULLIF(d.avg_monthly_demand, 0) < 1 THEN 'LOW'
    WHEN s.stock_qty / NULLIF(d.avg_monthly_demand, 0) > 4 THEN 'HIGH'
    ELSE 'OK'
  END                                                     AS stock_status
FROM latest_stock s
JOIN mart.dim_product p ON s.product_key = p.product_key
LEFT JOIN demand d      ON s.product_key = d.product_key;

CREATE OR REPLACE VIEW mart.v_sales_mom AS
SELECT
  product_code,
  year_month,
  total_qty,
  LAG(total_qty) OVER (PARTITION BY product_code ORDER BY month_start) AS prev_month_qty,
  ROUND(
    (total_qty - LAG(total_qty) OVER (PARTITION BY product_code ORDER BY month_start))
    / NULLIF(LAG(total_qty) OVER (PARTITION BY product_code ORDER BY month_start), 0) * 100
  , 1) AS mom_growth_pct
FROM mart.v_monthly_sales;


/* ---------------------------------------------------------------------
   7. Extra Snowflake features
   --------------------------------------------------------------------- */

-- 7.1 Zero-copy clone + MERGE (upsert) on the clone, original stays untouched
CREATE OR REPLACE TABLE mart.dim_product_demo CLONE mart.dim_product;

MERGE INTO mart.dim_product_demo t
USING (
  SELECT 'EL1001' AS product_code, 'Wireless Mouse Pro' AS product_name
  UNION ALL
  SELECT 'NEW9999', 'New Item'
) s
ON t.product_code = s.product_code
WHEN MATCHED THEN
  UPDATE SET t.product_name = s.product_name
WHEN NOT MATCHED THEN
  INSERT (product_key, product_code, product_name, category)
  VALUES (9999, s.product_code, s.product_name, 'UNKNOWN');

SELECT * FROM mart.dim_product_demo
WHERE product_code IN ('EL1001', 'NEW9999');

-- 7.2 Time Travel: undo an accidental DROP
CREATE OR REPLACE TABLE mart.fact_sales_test CLONE mart.fact_sales;
DROP TABLE mart.fact_sales_test;
UNDROP TABLE mart.fact_sales_test;
SELECT COUNT(*) AS rows_after_undrop FROM mart.fact_sales_test;

-- 7.3 Time Travel: query data as it was before a DELETE
DELETE FROM mart.fact_sales_test WHERE region = 'NORTH';
SELECT COUNT(*) AS rows_now FROM mart.fact_sales_test;
-- wait about 1 minute, then run:
SELECT COUNT(*) AS rows_one_minute_ago FROM mart.fact_sales_test AT (OFFSET => -60);

-- 7.4 Semi-structured data: VARIANT + FLATTEN
CREATE OR REPLACE TABLE raw.json_demo (v VARIANT);
INSERT INTO raw.json_demo
  SELECT PARSE_JSON('{"order":"A1","items":[{"sku":"X1","qty":2},{"sku":"X2","qty":5}]}');

SELECT
  v:order::STRING     AS order_id,
  i.value:sku::STRING AS sku,
  i.value:qty::INT    AS qty
FROM raw.json_demo,
     LATERAL FLATTEN(input => v:items) i;

-- 7.5 Clean up demo objects and stop spending credits
DROP TABLE IF EXISTS mart.dim_product_demo;
DROP TABLE IF EXISTS mart.fact_sales_test;
DROP TABLE IF EXISTS raw.json_demo;
ALTER WAREHOUSE sop_wh SUSPEND;
