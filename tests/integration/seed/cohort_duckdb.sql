-- Peer-cohort seed (tier 1, DuckDB in-process).
--
-- Every number below is chosen so the expected cohort result can be computed
-- BY HAND from this file alone. The comparison window is
-- 2025-01-01 .. 2025-03-31 — exactly 90 calendar days.
--
-- Per-store parameters (daily row values, repeated for `trading_days` days):
--
--   store    basis    daily_net  daily_wages  days | total_net  total_wages  wage_pct  norm
--   -------- -------- ---------  -----------  ---- | ---------  -----------  --------  ------
--   store_a  accrual      1000          300     90 |    90000        27000      0.30    1000
--   store_b  accrual      1000          220     90 |    90000        19800      0.22    1000
--   store_c  accrual      1100          264     90 |    99000        23760      0.24    1100
--   store_d  accrual      1300          403     90 |   117000        36270      0.31    1300
--   store_e  accrual      1900          342     90 |   171000        30780      0.18    1900
--   store_f  (NULL)       1000          250     90 |    90000        22500      0.25    1000
--   store_g  accrual     10000         3000      9 |    90000        27000      0.30   10000
--   store_h  cash         1000          280     90 |    90000        25200      0.28    1000
--
--   norm = total_net / trading_days, the band's normalised size axis
--          (`band.measure / band.per`).
--
-- The band is +-35%, so a subject at `norm` admits peers in
-- [0.65*norm, 1.35*norm], AND only peers sharing its `accounting_basis`
-- exactly, AND never itself (`exclude_self` defaults true):
--
--   store_a: [ 650,  1350] & accrual -> b(1000), c(1100), d(1300)   = 3 peers
--   store_d: [ 845,  1755] & accrual -> a, b, c  (e at 1900 is out) = 3 peers
--   store_e: [1235,  2565] & accrual -> d(1300)                     = 1 peer
--   store_g: [6500, 13500] & accrual -> nobody                      = 0 peers
--   store_h: cash, a population of one                              = 0 peers
--   store_f: accounting_basis IS NULL -> excluded before matching
--
-- The four cases this seed exists to engineer:
--
--   1. NORMALISATION. store_a and store_g have IDENTICAL 90-day totals
--      (90000 each) but 10x different trading-day counts (90 vs 9). Banding
--      on the raw total would make them peers; banding on net_sales per
--      trading day puts them 10x apart, in each other's blind spot.
--   2. ASYMMETRY. store_d (1300) is inside store_e's band [1235, 2565], but
--      store_e (1900) is OUTSIDE store_d's band [845, 1755]. Membership is
--      directional by design; a bucketing implementation would give the pair
--      one shared verdict.
--   3. EXCLUSION. store_f's accounting_basis is NULL. It matches nobody
--      exactly, so it is reported in `excluded` — and is nobody's peer.
--   4. INSUFFICIENCY. store_e has exactly 1 peer against `min_peers: 3`, so
--      it is returned with `sufficient: false`, NOT filtered away.
--
--   5. `require` really partitions. store_h sits at norm 1000, numerically
--      identical to store_a's own band centre — it would be one of store_a's
--      peers if the exact-match tuple were ignored. Its basis is `cash`.
--
--   6. ORPHANED FACT ROW. `store_zzz` has sales rows but NO row in `stores`
--      (a store deleted from the dimension table, or a late-arriving key —
--      routine on a real warehouse). The entity-grain pull is
--      `FROM sales_daily LEFT JOIN stores` (the fact view owns every measure
--      the pull names, so it wins `pick_base_view`, and a ManyToOne hop
--      always compiles to LEFT), so the orphan survives the join and
--      `GROUP BY key` emits it as ONE extra group with a NULL key.
--
--      It is NOT a ninth entity, and must not be counted as one: the
--      independent `COUNT(DISTINCT stores.store_id)` guard query has the
--      OPPOSITE base view and skips NULLs, so it still reports 8. The
--      NULL-key group is partitioned off before that cross-check and
--      reported in `excluded` under the key `(null)`.
--
--      Its numbers are deliberately irrelevant — it is never a peer and
--      never a subject — so it moves none of the arithmetic above: the
--      per-store totals, bands, peer sets, medians and gaps are all
--      unchanged by its presence. What it changes is the SIZE of `excluded`:
--      two entries (store_f and the NULL-key row), not one.
--
-- Hand-computed expectation for store_a, the assertion this file is built for:
--   peers  = [store_b, store_c, store_d] -> wage_pct [0.22, 0.24, 0.31]
--   median = 0.24  (R-7 of a 3-element set is the middle element)
--   value  = 0.30
--   gap    = value - baseline = 0.06   (wage_pct is lower_is_better)

CREATE TABLE stores (
    store_id VARCHAR PRIMARY KEY,
    accounting_basis VARCHAR
);

INSERT INTO stores VALUES
    ('store_a', 'accrual'),
    ('store_b', 'accrual'),
    ('store_c', 'accrual'),
    ('store_d', 'accrual'),
    ('store_e', 'accrual'),
    ('store_f', NULL),
    ('store_g', 'accrual'),
    ('store_h', 'cash');

CREATE TABLE sales_daily (
    store_id VARCHAR,
    sale_date DATE,
    net_sales BIGINT,
    wages BIGINT
);

-- One row per store per trading day, expanded from the parameter table above
-- so the seed stays hand-checkable instead of 639 opaque literal rows.
INSERT INTO sales_daily
SELECT p.store_id,
       CAST(DATE '2025-01-01' + CAST(d.day_offset AS INTEGER) AS DATE),
       p.daily_net,
       p.daily_wages
FROM (VALUES
        ('store_a',  1000,  300, 90),
        ('store_b',  1000,  220, 90),
        ('store_c',  1100,  264, 90),
        ('store_d',  1300,  403, 90),
        ('store_e',  1900,  342, 90),
        ('store_f',  1000,  250, 90),
        ('store_g', 10000, 3000,  9),
        ('store_h',  1000,  280, 90)
     ) AS p(store_id, daily_net, daily_wages, trading_days)
CROSS JOIN range(0, 90) AS d(day_offset)
WHERE d.day_offset < p.trading_days;

-- The orphaned fact row (case 6 in the header): a sale for a store that has
-- no row in `stores`. Inside the comparison window on purpose — outside it,
-- the pull would never see it and it would test nothing.
INSERT INTO sales_daily VALUES
    ('store_zzz', DATE '2025-02-01', 500, 100);
