/* =====================================================================
   Generate synthetic RAW data directly in Snowflake (no file upload)
   Fills: raw.sales_raw (~5000 rows) and raw.inventory_raw (~130 rows)
   Includes deliberate data quality problems: duplicates, bad dates,
   negative values, missing codes, messy product code formats, orphans.
   Run all statements with "Run all".
   ===================================================================== */

USE WAREHOUSE sop_wh;
USE DATABASE sop_db;

-- Start clean
TRUNCATE TABLE raw.sales_raw;
TRUNCATE TABLE raw.inventory_raw;


/* ---------------------------------------------------------------------
   1. Product master used only to generate data (22 products)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE raw.tmp_products AS
SELECT
  ROW_NUMBER() OVER (ORDER BY code) AS pidx,
  code, name, category, price
FROM VALUES
  ('EL1001','Wireless Mouse','Electronics',25),
  ('EL1002','USB Hub','Electronics',18),
  ('EL1003','Keyboard','Electronics',45),
  ('EL1004','Webcam','Electronics',60),
  ('EL1005','Headset','Electronics',55),
  ('EL1006','Monitor Stand','Electronics',30),
  ('EL1007','Charger','Electronics',15),
  ('EL1008','Power Bank','Electronics',35),
  ('FU1009','Office Chair','Furniture',120),
  ('FU1010','Desk Lamp','Furniture',40),
  ('FU1011','Bookshelf','Furniture',90),
  ('FU1012','Filing Cabinet','Furniture',110),
  ('FU1013','Standing Desk','Furniture',300),
  ('FU1014','Footrest','Furniture',25),
  ('SU1015','Notebook','Supplies',4),
  ('SU1016','Pen Set','Supplies',12),
  ('SU1017','Stapler','Supplies',9),
  ('SU1018','Whiteboard','Supplies',45),
  ('SU1019','Marker Pack','Supplies',8),
  ('SU1020','Paper Box','Supplies',30),
  ('SU1021','Folder Pack','Supplies',6),
  ('SU1022','Tape Roll','Supplies',3)
  AS t(code, name, category, price);


/* ---------------------------------------------------------------------
   2. Random base values (materialized once, so RANDOM() is not re-evaluated)
   Dates: 2025-01-01 .. 2026-09-30
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE raw.tmp_base AS
SELECT
  ROW_NUMBER() OVER (ORDER BY SEQ4())                       AS i,
  UNIFORM(1, 22, RANDOM())                                  AS pidx,
  DATEADD(day, UNIFORM(0, 636, RANDOM()), '2025-01-01'::DATE) AS d,
  UNIFORM(1, 4, RANDOM())                                   AS ridx,
  UNIFORM(1, 12, RANDOM())                                  AS q,
  UNIFORM(90, 110, RANDOM()) / 100.0                        AS price_factor,
  UNIFORM(1, 1000, RANDOM())                                AS noise,   -- decides which problem to inject
  UNIFORM(1, 100, RANDOM())                                 AS mess     -- decides product code format
FROM TABLE(GENERATOR(ROWCOUNT => 5000));


/* ---------------------------------------------------------------------
   3. Sales rows (all values as VARCHAR, like a real raw layer)
   --------------------------------------------------------------------- */
INSERT INTO raw.sales_raw
  (order_id, order_date, product_code, product_name, category, region, quantity, sales_amount)
SELECT
  'ORD' || LPAD(b.i::STRING, 5, '0'),
  CASE WHEN b.noise <= 3 THEN '2026-13-45'                         -- invalid date
       ELSE TO_CHAR(b.d, 'YYYY-MM-DD') END,
  CASE WHEN b.noise BETWEEN 10 AND 13 THEN NULL                    -- missing code
       WHEN b.mess <= 80 THEN p.code
       WHEN b.mess <= 88 THEN LOWER(p.code)                        -- lower case
       WHEN b.mess <= 94 THEN ' ' || p.code || ' '                 -- spaces
       ELSE SUBSTR(p.code, 1, 2) || '-' || SUBSTR(p.code, 3) END,  -- dash
  p.name,
  p.category,
  CASE b.ridx WHEN 1 THEN 'North' WHEN 2 THEN 'South'
              WHEN 3 THEN 'East'  ELSE 'West' END,
  (CASE WHEN b.noise BETWEEN 4 AND 9 THEN -b.q ELSE b.q END)::STRING,   -- negative quantity
  (CASE WHEN b.noise BETWEEN 14 AND 18 THEN -1 ELSE 1 END
     * ROUND(b.q * p.price * b.price_factor, 2))::STRING                -- negative amount
FROM raw.tmp_base b
JOIN raw.tmp_products p ON p.pidx = b.pidx;

-- Duplicate rows (exact copies of 25 random existing rows)
INSERT INTO raw.sales_raw
  (order_id, order_date, product_code, product_name, category, region, quantity, sales_amount)
SELECT order_id, order_date, product_code, product_name, category, region, quantity, sales_amount
FROM raw.sales_raw SAMPLE (25 ROWS);


/* ---------------------------------------------------------------------
   4. Inventory rows: 20 products x 2 warehouses x 3 month-end snapshots
   (the last 2 products never appear in inventory)
   --------------------------------------------------------------------- */
INSERT INTO raw.inventory_raw (product_code, warehouse, snapshot_date, stock_qty)
SELECT
  CASE MOD(p.pidx, 5)
    WHEN 1 THEN LOWER(p.code)
    WHEN 2 THEN ' ' || p.code || ' '
    WHEN 3 THEN SUBSTR(p.code, 1, 2) || '-' || SUBSTR(p.code, 3)
    ELSE p.code END,
  w.wh,
  TO_CHAR(s.snap, 'YYYY-MM-DD'),
  UNIFORM(20, 900, RANDOM())::STRING
FROM raw.tmp_products p
CROSS JOIN (SELECT column1 AS wh FROM VALUES ('WH-HELSINKI'), ('WH-TAMPERE')) w
CROSS JOIN (SELECT column1 AS snap
            FROM VALUES ('2026-07-31'::DATE), ('2026-08-31'::DATE), ('2026-09-30'::DATE)) s
WHERE p.pidx <= 20;

-- Orphan product codes (exist only in inventory)
INSERT INTO raw.inventory_raw (product_code, warehouse, snapshot_date, stock_qty)
SELECT column1, column2, column3, column4
FROM VALUES
  ('ZZ9000', 'WH-HELSINKI', '2026-09-30', '55'),
  ('ZZ9001', 'WH-HELSINKI', '2026-09-30', '80'),
  ('ZZ9002', 'WH-TAMPERE',  '2026-09-30', '12');

-- Negative stock rows (data entry errors)
INSERT INTO raw.inventory_raw (product_code, warehouse, snapshot_date, stock_qty)
SELECT column1, column2, column3, column4
FROM VALUES
  ('EL1003', 'WH-TAMPERE',  '2026-06-30', '-15'),
  ('FU1010', 'WH-HELSINKI', '2026-06-30', '-40'),
  ('SU1016', 'WH-TAMPERE',  '2026-06-30', '-5'),
  ('SU1020', 'WH-HELSINKI', '2026-06-30', '-22');

-- Duplicate inventory rows
INSERT INTO raw.inventory_raw (product_code, warehouse, snapshot_date, stock_qty)
SELECT product_code, warehouse, snapshot_date, stock_qty
FROM raw.inventory_raw SAMPLE (6 ROWS);


/* ---------------------------------------------------------------------
   5. Clean up helper tables and verify
   --------------------------------------------------------------------- */
DROP TABLE IF EXISTS raw.tmp_base;
DROP TABLE IF EXISTS raw.tmp_products;

SELECT 'sales_raw'     AS table_name, COUNT(*) AS row_count FROM raw.sales_raw
UNION ALL
SELECT 'inventory_raw' AS table_name, COUNT(*) AS row_count FROM raw.inventory_raw;

SELECT * FROM raw.sales_raw LIMIT 10;
