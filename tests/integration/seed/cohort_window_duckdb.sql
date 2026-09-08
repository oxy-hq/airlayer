-- Peer-cohort BAND WINDOW seed (tier 1, DuckDB in-process).
--
-- Companion to `cohort_duckdb.sql`, kept separate so that fixture's
-- hand-computed census (8 stores, 2 exclusions) stays pristine. This one
-- engineers exactly one pathology: **the band and the metric want different
-- windows**.
--
-- The reporting period is ONE MONTH: 2025-03-01 .. 2025-03-31. That is a
-- noisy size proxy — a store that had a slow March is not a smaller store —
-- so the band is measured over a trailing window instead:
--
--   band window = [period_end - 90 days, period_end]
--               = [2024-12-31, 2025-03-31]
--
-- (2025-03-31 minus 90 calendar days IS 2024-12-31: Jan 31 + Feb 28 +
-- Mar 31 = 90.) All seeded rows fall on/after 2025-01-01, so the band window
-- covers every seeded row and the period covers only March.
--
-- Per-store parameters:
--
--   store    days in band window      daily net / wages           days in March
--   -------- ------------------------ --------------------------- -------------
--   store_p  2025-01-01..03-31 (90)   1000 / 300 throughout        31
--   store_q  2025-01-11..03-31 (80)   1200 / 360 until 2025-02-28  31
--                                      400 / 124 from 2025-03-01
--   store_r  2025-01-01..03-31 (90)   3000 / 900 throughout        31
--   store_s  2025-01-01..01-31 (31)   1000 / 300 throughout         0
--
-- BAND-WINDOW norms (`band.measure / band.per` = net_sales / trading_days,
-- both measured over [2024-12-31, 2025-03-31]):
--
--   store_p  90000 / 90 = 1000
--   store_q  (49*1200 + 31*400) / 80 = (58800 + 12400) / 80 = 71200 / 80 = 890
--   store_r  270000 / 90 = 3000
--   store_s  31000 / 31 = 1000
--
--   (store_q's 49 pre-March days are 21 in January + 28 in February.)
--
-- PERIOD norms (the same expression measured over March alone) — what
-- today's implementation, which has only one window, computes instead:
--
--   store_p  31000 / 31 = 1000
--   store_q  12400 / 31 =  400     <-- the whole point: a slow March
--   store_r  93000 / 31 = 3000
--   store_s  no rows in March at all
--
-- The band is +-35%, `exclude_self` defaults true, and there is deliberately
-- no `require` (the band alone defines this cohort):
--
--   TRAILING band (`window: 90 days`):
--     store_p at 1000 -> [ 650.0, 1350.0] -> store_q (890)   = 1 peer
--     store_q at  890 -> [ 578.5, 1201.5] -> store_p (1000)  = 1 peer
--     store_r at 3000 -> [1950.0, 4050.0] -> nobody          = 0 peers
--
--   PERIOD band (no `window:`, the same declaration otherwise):
--     store_p at 1000 -> [ 650.0, 1350.0] -> store_q is 400  = 0 peers
--     store_q at  400 -> [ 260.0,  540.0] -> store_p is 1000 = 0 peers
--     store_r at 3000 -> [1950.0, 4050.0] -> nobody          = 0 peers
--
-- So store_p and store_q are peers under the trailing band and are NOT peers
-- under the period band, in BOTH directions. That pair is the assertion this
-- file exists for, and it fails against an implementation that measures the
-- band over the query period.
--
-- The compared measure, `wage_pct` (lower_is_better), is measured over the
-- PERIOD in both cases — the band's window must not drag the metric with it:
--
--   store_p  9300 / 31000 = 0.30
--   store_q  3844 / 12400 = 0.31
--   store_r 27900 / 93000 = 0.30
--
-- Hand-computed expectation under the trailing band, `min_peers: 1`:
--   store_p: peers [store_q], baseline = median([0.31]) = 0.31, value 0.30,
--            gap = value - baseline = -0.01 (lower_is_better; store_p is
--            AHEAD of its peer, so the gap is negative), sufficient = true
--   store_q: peers [store_p], baseline 0.30, value 0.31, gap = +0.01
--   store_r: 0 peers, baseline 0.0, gap 0.0, sufficient = false
--
-- THE TWO PULLS ARE DIFFERENT POPULATIONS. `store_s` exists for that:
--
--   7. WINDOW-ONLY ENTITY. store_s traded in January and never again. It is
--      in the BAND-window pull (norm 1000, numerically identical to
--      store_p's own band centre, so a naive implementation would hand
--      store_p a second peer) and absent from the PERIOD pull, so it has no
--      value to compare and none to contribute to anyone's baseline. It must
--      be REPORTED in `excluded` with that reason — not silently dropped,
--      and not admitted to store_p's peer set.
--
--      It also makes the two guard counts differ on purpose: the independent
--      COUNT(DISTINCT store_id) is 3 over March and 4 over the band window,
--      so each pull must be cross-checked against ITS OWN count. Checking
--      both against one count would refuse this fixture outright.

CREATE TABLE stores (
    store_id VARCHAR PRIMARY KEY
);

INSERT INTO stores VALUES
    ('store_p'),
    ('store_q'),
    ('store_r'),
    ('store_s');

CREATE TABLE sales_daily (
    store_id VARCHAR,
    sale_date DATE,
    net_sales BIGINT,
    wages BIGINT
);

-- One row per store per trading day, expanded from the parameter table above
-- so the seed stays hand-checkable. `first_offset`/`last_offset` are day
-- offsets from 2025-01-01 (so 0 = Jan 1, 59 = Mar 1, 89 = Mar 31), and the
-- rate switches at `switch_offset`: rows before it carry the `early_*`
-- values, rows on/after it the `late_*` ones. A store with no rate change
-- sets `switch_offset` past the end.
INSERT INTO sales_daily
SELECT p.store_id,
       CAST(DATE '2025-01-01' + CAST(d.day_offset AS INTEGER) AS DATE),
       CASE WHEN d.day_offset < p.switch_offset THEN p.early_net   ELSE p.late_net   END,
       CASE WHEN d.day_offset < p.switch_offset THEN p.early_wages ELSE p.late_wages END
FROM (VALUES
        --         first last switch early_net early_wages late_net late_wages
        ('store_p',    0,  89,   999,     1000,        300,    1000,       300),
        ('store_q',   10,  89,    59,     1200,        360,     400,       124),
        ('store_r',    0,  89,   999,     3000,        900,    3000,       900),
        ('store_s',    0,  30,   999,     1000,        300,    1000,       300)
     ) AS p(store_id, first_offset, last_offset, switch_offset,
            early_net, early_wages, late_net, late_wages)
CROSS JOIN range(0, 90) AS d(day_offset)
WHERE d.day_offset >= p.first_offset
  AND d.day_offset <= p.last_offset;
