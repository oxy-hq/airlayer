-- Seed data for the same-store-sales demo (the acceptance model).
-- Load with:  duckdb same-store-sales.duckdb < seed.sql
DROP TABLE IF EXISTS stores;
DROP TABLE IF EXISTS sales_daily;

CREATE TABLE stores (
    store_id  VARCHAR,
    region    VARCHAR,
    opened_at DATE,
    closed_at DATE
);
INSERT INTO stores VALUES
  ('A','East', '2021-01-01', NULL),         -- open the whole time
  ('B','East', '2023-01-01', NULL),         -- open the whole time
  ('C','West', '2025-07-01', NULL),         -- opened mid-prior-year (new store)
  ('D','West', '2026-02-01', NULL),         -- opened in the current year (new store)
  ('E','South','2019-01-01', '2026-09-15'); -- closed before the current year ends

-- Daily rows (two per store-year) summing to the annual net_sales totals.
CREATE TABLE sales_daily (
    store_id          VARCHAR,
    sale_date         DATE,
    net_sales         INTEGER,
    transaction_count INTEGER
);
INSERT INTO sales_daily VALUES
  -- A: 2025=1000, 2026=980
  ('A','2025-01-15',500,50),('A','2025-07-15',500,50),
  ('A','2026-01-15',490,49),('A','2026-07-15',490,49),
  -- B: 2025=1200, 2026=1150
  ('B','2025-01-15',600,60),('B','2025-07-15',600,60),
  ('B','2026-01-15',575,57),('B','2026-07-15',575,58),
  -- C: opened mid-2025. 2025=400, 2026=850
  ('C','2025-08-15',200,20),('C','2025-10-15',200,20),
  ('C','2026-01-15',425,42),('C','2026-07-15',425,43),
  -- D: opened 2026. 2026=500
  ('D','2026-03-15',250,25),('D','2026-07-15',250,25),
  -- E: closed 2026-09-15. 2025=900, 2026=700
  ('E','2025-01-15',450,45),('E','2025-07-15',450,45),
  ('E','2026-01-15',350,35),('E','2026-08-15',350,35);
