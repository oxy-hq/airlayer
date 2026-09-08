-- Peer-cohort BAND WINDOW seed (tier 1, DuckDB in-process).
--
-- Companion to `cohort_duckdb.sql`, kept separate so that fixture's
-- hand-computed census (8 stores, 2 exclusions) stays pristine. This one
-- engineers two pathologies at once:
--
--   (a) **the band and the metric want different windows**, and
--   (b) **the band window is anchored at the period START, not its end**.
--
-- The reporting period is ONE MONTH: 2025-03-01 .. 2025-03-31. That is a
-- noisy size proxy — a store that had a slow March is not a smaller store —
-- so the band is measured over a trailing window instead. The window is a
-- lookback EXTENSION of the period, anchored at its start:
--
--   band window = [period_start - 90 days, period_end]
--               = [2024-12-01, 2025-03-31]
--
-- (2025-03-01 minus 90 calendar days IS 2024-12-01: Dec 31 + Jan 31 +
-- Feb 28 = 90.) The end-anchored window this fixture also discriminates
-- against would be [2025-03-31 - 90 days, 2025-03-31] = [2024-12-31,
-- 2025-03-31] (Jan 31 + Feb 28 + Mar 31 = 90). The two differ by exactly the
-- 30 days 2024-12-01 .. 2024-12-30, and `store_t` below trades in that gap
-- and nowhere else before March.
--
-- Per-store parameters (segments, each a closed day range at a flat rate):
--
--   store    trading segments                    daily net / wages
--   -------- ---------------------------------- ------------------
--   store_p  2025-01-01..2025-03-31 (90 days)    1000 / 300
--   store_q  2025-01-11..2025-02-28 (49 days)    1200 / 360
--            2025-03-01..2025-03-31 (31 days)     400 / 124
--   store_r  2025-01-01..2025-03-31 (90 days)    3000 / 900
--   store_s  2025-01-01..2025-01-31 (31 days)    1000 / 300
--   store_t  2024-12-01..2024-12-30 (30 days)    1930 / 579
--            2025-03-01..2025-03-31 (31 days)     100 /  31
--
-- BAND-WINDOW norms (`band.measure / band.per` = net_sales / trading_days,
-- both measured over the START-ANCHORED [2024-12-01, 2025-03-31]):
--
--   store_p  90000 / 90 = 1000
--   store_q  (49*1200 + 31*400) / 80 = (58800 + 12400) / 80 = 71200 / 80 = 890
--   store_r  270000 / 90 = 3000
--   store_s  31000 / 31 = 1000
--   store_t  (30*1930 + 31*100) / 61 = (57900 + 3100) / 61 = 61000 / 61 = 1000
--
--   (store_q's 49 pre-March days are 21 in January + 28 in February.)
--
-- END-ANCHORED norms (the same expression over [2024-12-31, 2025-03-31]) —
-- what an implementation anchored at the period end computes instead. Only
-- store_t moves, and it moves by a factor of ten, because December 1-30 is
-- exactly the span the two anchors disagree about:
--
--   store_p  90000 / 90 = 1000     (unchanged)
--   store_q  71200 / 80 =  890     (unchanged)
--   store_r 270000 / 90 = 3000     (unchanged)
--   store_s  31000 / 31 = 1000     (unchanged)
--   store_t   3100 / 31 =  100     <-- its December trading is outside
--
-- PERIOD norms (the same expression measured over March alone) — what an
-- implementation with only one window computes instead:
--
--   store_p  31000 / 31 = 1000
--   store_q  12400 / 31 =  400     <-- a slow March
--   store_r  93000 / 31 = 3000
--   store_s  no rows in March at all
--   store_t   3100 / 31 =  100
--
-- The band is +-35%, `exclude_self` defaults true, and there is deliberately
-- no `require` (the band alone defines this cohort). Subjects are the
-- entities present in BOTH pulls: store_p, store_q, store_r, store_t.
--
--   START-ANCHORED trailing band (`window: 90 days`) — what must happen:
--     store_p at 1000 -> [ 650.0, 1350.0] -> store_q (890), store_t (1000)
--     store_q at  890 -> [ 578.5, 1201.5] -> store_p (1000), store_t (1000)
--     store_r at 3000 -> [1950.0, 4050.0] -> nobody
--     store_t at 1000 -> [ 650.0, 1350.0] -> store_p (1000), store_q (890)
--
--   END-ANCHORED trailing band — the same declaration, wrong anchor:
--     store_p at 1000 -> [ 650.0, 1350.0] -> store_q (890) ONLY; store_t
--                                            has fallen to 100
--     store_t at  100 -> [  65.0,  135.0] -> nobody
--
--   PERIOD band (no `window:`, the same declaration otherwise):
--     store_p at 1000 -> [ 650.0, 1350.0] -> store_q is 400, store_t is 100
--                                            = 0 peers
--     store_q at  400 -> [ 260.0,  540.0] -> store_p is 1000, store_t is 100
--                                            = 0 peers
--     store_r at 3000 -> [1950.0, 4050.0] -> nobody          = 0 peers
--     store_t at  100 -> [  65.0,  135.0] -> nobody          = 0 peers
--
-- So the fixture discriminates on BOTH axes with one seed:
--   * store_p and store_q are peers under the trailing band and NOT peers
--     under the period band, in both directions (trailing vs period), and
--   * store_t is store_p's peer under the START-anchored window and NOT under
--     the END-anchored one, in both directions (start vs end anchor).
--
-- The compared measure, `wage_pct` (lower_is_better), is measured over the
-- PERIOD in every case — the band's window must not drag the metric with it:
--
--   store_p  9300 / 31000 = 0.30
--   store_q  3844 / 12400 = 0.31
--   store_r 27900 / 93000 = 0.30
--   store_t   961 /  3100 = 0.31
--
-- (store_t's December wages, 579/day, are never read: December is outside the
-- comparison period, and the band reads net_sales and trading days only.)
--
-- Hand-computed expectation under the start-anchored trailing band,
-- `min_peers: 1`, `statistic: median` (R-7, so the median of two values is
-- their mean):
--   store_p: peers [store_q, store_t], baseline = median([0.31, 0.31]) = 0.31,
--            value 0.30, gap = value - baseline = -0.01 (lower_is_better;
--            store_p is AHEAD of its peers, so the gap is negative),
--            sufficient = true
--   store_q: peers [store_p, store_t], baseline = median([0.30, 0.31]) = 0.305,
--            value 0.31, gap = +0.005, sufficient = true
--   store_r: 0 peers, baseline 0.0, gap 0.0, sufficient = false
--   store_t: peers [store_p, store_q], baseline = median([0.30, 0.31]) = 0.305,
--            value 0.31, gap = +0.005, sufficient = true
--
-- THE TWO PULLS ARE DIFFERENT POPULATIONS. `store_s` exists for that:
--
--   WINDOW-ONLY ENTITY. store_s traded in January and never again. It is in
--   the BAND-window pull (norm 1000, numerically identical to store_p's own
--   band centre, so a naive implementation would hand store_p a third peer)
--   and absent from the PERIOD pull, so it has no value to compare and none
--   to contribute to anyone's baseline. It must be REPORTED in `excluded`
--   with that reason — not silently dropped, and not admitted to store_p's
--   peer set. Start-anchoring makes the band window contain the period, so
--   this direction is the only one the WINDOWS alone can produce; the
--   opposite direction survives because the two pulls select different
--   measures, not because one window is shorter.
--
--   It also makes the two guard counts differ on purpose: the independent
--   COUNT(DISTINCT store_id) is 4 over March (p, q, r, t) and 5 over the band
--   window (all five), so each pull must be cross-checked against ITS OWN
--   count. Checking both against one count would refuse this fixture
--   outright.

CREATE TABLE stores (
    store_id VARCHAR PRIMARY KEY
);

INSERT INTO stores VALUES
    ('store_p'),
    ('store_q'),
    ('store_r'),
    ('store_s'),
    ('store_t');

CREATE TABLE sales_daily (
    store_id VARCHAR,
    sale_date DATE,
    net_sales BIGINT,
    wages BIGINT
);

-- One row per store per trading day, expanded from the segment table above so
-- the seed stays hand-checkable. `first_offset`/`last_offset` are inclusive
-- day offsets from 2025-01-01, so 0 = Jan 1, 59 = Mar 1, 89 = Mar 31, and
-- -31 = 2024-12-01. A store with a rate change (or a gap in its trading)
-- contributes one row per segment; the ranges within a store are disjoint, so
-- no day is emitted twice.
INSERT INTO sales_daily
SELECT p.store_id,
       CAST(DATE '2025-01-01' + CAST(d.day_offset AS INTEGER) AS DATE),
       p.net,
       p.wages
FROM (VALUES
        --         first last   net wages
        ('store_p',    0,  89, 1000,  300),
        ('store_q',   10,  58, 1200,  360),
        ('store_q',   59,  89,  400,  124),
        ('store_r',    0,  89, 3000,  900),
        ('store_s',    0,  30, 1000,  300),
        -- 2024-12-01 .. 2024-12-30: inside the start-anchored band window,
        -- outside the end-anchored one. This segment is the discriminator.
        ('store_t',  -31,  -2, 1930,  579),
        ('store_t',   59,  89,  100,   31)
     ) AS p(store_id, first_offset, last_offset, net, wages)
CROSS JOIN range(-31, 90) AS d(day_offset)
WHERE d.day_offset >= p.first_offset
  AND d.day_offset <= p.last_offset;
