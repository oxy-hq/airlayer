# Same-store sales: `lifespan` + `shift`

A worked example of airlayer's two comparison primitives. Same-store sales —
a notoriously fiddly metric — falls out as a plain ratio of primitives, with
**zero per-query date arithmetic**.

## The model

- **`stores.view.yml`** — each store declares a `lifespan` once (`opened_at` /
  `closed_at`). That's the only cohort bookkeeping anywhere.
- **`sales.view.yml`** — `net_sales_prior` is a `shift` that re-evaluates
  `net_sales` one year earlier and restricts the query to the comparable cohort
  via `comparable_by: store_id`. `same_store_sales` is then just:

  ```yaml
  expr: "{{sales.net_sales}} / NULLIF({{sales.net_sales_prior}}, 0) - 1"
  ```

`comparable_by: store_id` names the entity whose `lifespan` defines
comparability — the query is restricted to stores live across **both** windows.
Drop it and you get plain year-over-year (every store counts); it's the one
field that turns period-over-period into same-store.

## Run it

```bash
# from this directory (needs the `duckdb` CLI to seed on first run):
./demo.sh
```

Or step through it manually with plain CLI flags. `--time-dimension` takes
`member:granularity:from,to` and supplies the current window the shift compares
against:

```bash
# 1. seed the DuckDB file
duckdb same-store-sales.duckdb < seed.sql

# 2. compile the comp to SQL (no DB needed)
airlayer query --config config.yml -d duckdb \
  --measure sales.same_store_sales \
  --measure sales.net_sales \
  --measure sales.net_sales_prior \
  --time-dimension sales.sale_date:year:2026-01-01,2026-12-31

# 3. execute it (swap `query` → `query -x`)
airlayer query -x --config config.yml \
  --measure sales.same_store_sales \
  --measure sales.net_sales \
  --measure sales.net_sales_prior \
  --time-dimension sales.sale_date:year:2026-01-01,2026-12-31
```

## Expected result

```
sales__same_store_sales ≈ -0.0318   (-3.18%)
sales__net_sales        = 2130       (A:980 + B:1150)
sales__net_sales_prior  = 2200       (A:1000 + B:1200)
```

Only **A** and **B** are comparable across FY2025 and FY2026:

| store | opened | closed | comparable? |
|-------|--------|--------|-------------|
| A | 2021-01-01 | — | ✅ |
| B | 2023-01-01 | — | ✅ |
| C | 2025-07-01 | — | ❌ opened mid-prior-year (new store) |
| D | 2026-02-01 | — | ❌ opened in the current year |
| E | 2019-01-01 | 2026-09-15 | ❌ closed before the current year ends |

C and D are excluded as new stores; **E** is excluded by the two-sided check
(open early enough, but closed before the current window ends). The comp is
`2130 / 2200 - 1 = -3.18%` — uncontaminated by stores that didn't exist or had
closed in one of the windows.

## Discover it programmatically

```bash
airlayer inspect --json   # surfaces each shift (base, by, direction,
                          # comparable_by, maturity) and entity lifespans
```
