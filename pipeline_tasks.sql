/* =====================================================================
   Pipeline automation for the Mini S&OP project
   - ops.pipeline_log           : run log (step, status, rows, message)
   - ops.sp_refresh_staging()   : rebuilds cleaned + rejected tables
   - ops.sp_refresh_mart()      : rebuilds dim/fact tables, runs reconciliation
   - ops.t_refresh_staging      : root task, runs every day at 05:00 (Helsinki)
   - ops.t_refresh_mart         : child task, runs AFTER the staging task
   Tasks are created SUSPENDED. Resume them only when you want them to run.
   All tasks of one task graph must live in the same schema -> schema OPS.
   ===================================================================== */

USE WAREHOUSE sop_wh;
USE DATABASE sop_db;

CREATE SCHEMA IF NOT EXISTS ops;


/* ---------------------------------------------------------------------
   1. Log table
   --------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS ops.pipeline_log (
  run_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  step          VARCHAR,
  status        VARCHAR,
  rows_affected NUMBER,
  message       VARCHAR
);


/* ---------------------------------------------------------------------
   2. Procedure: refresh STAGING
   --------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE ops.sp_refresh_staging()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
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

  INSERT INTO ops.pipeline_log (step, status, rows_affected, message)
    SELECT 'staging', 'SUCCESS', COUNT(*), 'rows in staging.sales_clean'
    FROM staging.sales_clean;

  RETURN 'staging OK';

EXCEPTION
  WHEN OTHER THEN
    INSERT INTO ops.pipeline_log (step, status, message)
      VALUES ('staging', 'FAILED', :SQLERRM);
    RAISE;
END;
$$;


/* ---------------------------------------------------------------------
   3. Procedure: refresh MART + reconciliation
      If rows are lost without a reason, the run FAILS (and is logged).
   --------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE ops.sp_refresh_mart()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  unexplained  NUMBER;
  recon_failed EXCEPTION (-20001, 'Reconciliation failed: unexplained rows in fact_sales');
BEGIN
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

  -- Reconciliation: clean rows = loaded rows + rejected rows
  unexplained := (
    SELECT
      (SELECT COUNT(*) FROM staging.sales_clean)
      - (SELECT COUNT(*) FROM mart.fact_sales)
      - (SELECT COUNT(*) FROM staging.sales_rejected)
  );

  IF (unexplained <> 0) THEN
    RAISE recon_failed;
  END IF;

  INSERT INTO ops.pipeline_log (step, status, rows_affected, message)
    SELECT 'mart', 'SUCCESS', COUNT(*), 'rows in mart.fact_sales (reconciliation = 0)'
    FROM mart.fact_sales;

  RETURN 'mart OK';

EXCEPTION
  WHEN OTHER THEN
    INSERT INTO ops.pipeline_log (step, status, message)
      VALUES ('mart', 'FAILED', :SQLERRM);
    RAISE;
END;
$$;


/* ---------------------------------------------------------------------
   4. Task graph: staging -> mart (created suspended)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TASK ops.t_refresh_staging
  WAREHOUSE = sop_wh
  SCHEDULE  = 'USING CRON 0 5 * * * Europe/Helsinki'
AS
  CALL ops.sp_refresh_staging();

CREATE OR REPLACE TASK ops.t_refresh_mart
  WAREHOUSE = sop_wh
  AFTER ops.t_refresh_staging
AS
  CALL ops.sp_refresh_mart();

SHOW TASKS IN SCHEMA sop_db.ops;
