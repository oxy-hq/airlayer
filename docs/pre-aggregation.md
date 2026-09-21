# Pre-Aggregation

Pre-aggregation materializes rollup tables from your views, dramatically reducing query times by scanning thousands of rows instead of millions.

## How it works

1. **Build** — `airlayer build` reads `pre_aggregations` from your `.view.yml` files and creates rollup tables in a dedicated schema (default: `AIRLAYER`) in your warehouse.
2. **Pull** — `airlayer pull` downloads those rollup tables to local Parquet files in `.airlayer/cache/`.
3. **Query** — an ad-hoc `airlayer query -x` automatically checks for a matching rollup before hitting the raw table (a saved `.query.yml` executed with `-x` does not — see below). Resolution order:
   1. Local Parquet cache (via DuckDB, instant)
   2. Warehouse rollup tables (via the original database connection)
   3. Raw SQL against the source table (fallback)

Use `--no-cache` to bypass both cache layers and always hit the raw table.

### Nothing resolves a rollup unless it asks to

This is the single most confusing thing about the subsystem, so it is stated
here rather than buried: **`SemanticEngine::compile_query` does no rollup
resolution at all.** It rewrites induced measures, clamps the limit, resolves
the dialect and generates raw SQL (`src/engine/mod.rs:263-316`) — it never
looks at a manifest. Every pre-aggregation short-circuit lives at a call site
that explicitly opts in:

| Entry point | Consults rollups? |
|---|---|
| `airlayer query -x` — `run_execute` (`src/cli/mod.rs:4768`), tiers at `:4893` and `:4990` | Yes, both tiers |
| WASM `cache_resolve` / `cache_resolve_warehouse` (`src/wasm.rs:280`, `:401`) | Yes |
| FFI `airlayer_cache_resolve` / `airlayer_cache_resolve_warehouse` (`src/ffi.rs:181`, `:299`) | Yes |
| `SemanticEngine::compile_query` called as a library | **No** |
| `airlayer query <file>.query.yml -x` — `run_saved_query_execute` (`src/cli/mod.rs:3849`) | **No** |
| `airlayer cohort`, `opportunity`, `explain`, `predict` | **No** |

The four analytical subcommands each build their own `QueryRequest` and run it
through a plain `compile_query` + `executor::execute` closure
(`src/cli/mod.rs:3185`, `:2911`, `:3361`, `:2791`) with no manifest lookup.
Executing a **saved query** takes the same shape (`src/cli/mod.rs:3890-3894`),
so `-x` consulting rollups is a property of the ad-hoc `query` command, not of
`-x`. All of these always read the raw table, and an embedder that wants
rollups has to call `preagg::resolve_local` / `resolve_cached` /
`resolve_warehouse` itself.

## Defining rollups

Pre-aggregation is **opt-in**. A view without a `pre_aggregations` block produces
no rollups and is skipped by `build` — there is no implicit default rollup. (An
all-dimensions rollup on a wide view is typically as large as the base table and
buys nothing, so the choice of grain is left to you.) If no view in the project
declares a block, `build` exits with an error saying so.

Add a `pre_aggregations` section to any `.view.yml` file:

```yaml
name: events
table: events
datasource: warehouse

dimensions:
  - name: platform
    type: string
    expr: platform
  - name: country
    type: string
    expr: country
  - name: created_at
    type: datetime
    expr: created_at

measures:
  - name: event_count
    type: count
  - name: total_revenue
    type: sum
    expr: revenue_cents / 100.0
  - name: avg_revenue
    type: average
    expr: revenue_cents / 100.0

pre_aggregations:
  - name: by_platform_daily
    dimensions: [platform]
    measures: [event_count, total_revenue, avg_revenue]
    time_dimension: created_at
    granularity: day

  - name: by_country_monthly
    dimensions: [country]
    measures: [event_count, total_revenue]
    time_dimension: created_at
    granularity: month
```

### Rollup fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique rollup name within the view |
| `dimensions` | string[] | No | Dimensions to GROUP BY in the rollup |
| `measures` | string[] | No | Measures to include (omitted = none) |
| `time_dimension` | string | No | Time dimension for date-based grouping |
| `granularity` | string | No | Time granularity. The string is passed straight to the dialect's `date_trunc` and nothing validates it, so `second`, `minute` and `hour` work alongside `day`, `week`, `month`, `quarter` and `year` |
| `refresh_key` | map | No | When `build` may skip this rollup as still fresh — see [Freshness](#freshness-refresh_key) |

### Omitted fields

`measures` and `dimensions` are both listed literally — there is no "all of them"
shorthand. Omitting `measures` produces a rollup with no measures (it covers no
measure query, so it is almost never what you want). Omitting `dimensions` rolls
the measures up over the time dimension alone.

### Eligible measure types

Two different things are worth separating: what `build` *stores*, and what
`covers()` will *answer from*. They are not the same list.

| Type | A rollup can answer it | Stored columns |
|------|---|----------------|
| `sum` | Yes | `measure__sum` |
| `count` | Yes | `measure__count` |
| `avg` / `average` | Yes | `measure__sum` + `measure__count` (re-divided at read time) |
| `min` | Yes | `measure__min` |
| `max` | Yes | `measure__max` |
| `count_distinct` | **Yes** | the raw expr column, added to the rollup's own `GROUP BY` |
| `count_distinct_approx` | **Yes** | the raw expr column, added to the rollup's own `GROUP BY` |
| `median` | No | raw expr column + `<expr>__freq` — built, then never read |
| `number` | No | `measure__value` — built, then never read |
| `custom` | No | none; nothing is stored |

`count_distinct` surprises people, so to be explicit: **it is pre-aggregable.**
The rollup stores the measure's raw expression as a grouping column and the
re-aggregation does `COUNT(DISTINCT …)` over it (`src/engine/preagg.rs:2406-2415`),
which is exact. `count_distinct_approx` is stored the same way and also
re-aggregates through an exact `COUNT(DISTINCT …)`, not the dialect's
approximate function.

`median`, `number` and `custom` are rejected at *query* time by `covers()`
(`src/engine/preagg.rs:2093`), and their columns are still written. For
`number` that column is simply inert. For `median` it is worse than inert: like
`count_distinct`, its raw column is added to the **build-time `GROUP BY`**
(`:764-797`), so it widens the rollup's on-disk grain and row count, and it
disqualifies the whole rollup from the exact-grain passthrough for every *other*
measure in it (`:2230-2237`). Leave both out of `measures:`.

There used to be a `median` re-aggregation arm that took a plain median of the
stored raw column while ignoring the `__freq` weights beside it; it was
removed, and `covers()` is now the only gate.

Some shapes are not a silent miss but a **build-time error**:

- a measure listed in `pre_aggregations.measures` that the view does not declare
  (`src/engine/preagg.rs:753-762`)
- a `rolling_window` measure, reached directly or through another member's expr —
  its value depends on rows outside the group a rollup stores
  (`src/engine/preagg.rs:678-687`)
- a `count_distinct` / `count_distinct_approx` / `median` whose expr contains a
  `{{…}}` reference, since that shape stores a raw *column* and a reference
  cannot name one (`src/engine/preagg.rs:777-783`)
- a member expr that reaches outside the view — another view, a foreign entity,
  or `{{variables.X}}`. A rollup CTAS reads one table with no joins, so the
  reference is refused by name rather than being passed through to the
  warehouse (`src/engine/preagg.rs:513-541`)

### Measure `filters:`

A measure's `filters:` block **is** honoured by pre-aggregation. It is folded
into the CTAS as the argument the aggregate is taken over — `SUM(CASE WHEN
<cond> THEN <expr> END)` (`filtered_inner`, `src/engine/preagg.rs:634-654`).

- A filtered `sum` is wrapped in `COALESCE(…, 0)`
  (`src/engine/preagg.rs:699-703`), because a filtered `SUM(CASE …)` is `NULL`
  when no row in the group matches and the live path answers `0`.
- A filtered `average` stores **both** `__sum` and `__count` filtered, and
  neither is coalesced — an all-`NULL` group must stay `NULL`, which is what
  `AVG` gives on the live path.
- `count_distinct`, `count_distinct_approx` and `median` **cannot** carry a
  filter. Those shapes store a raw column with no aggregate to fold a condition
  into, so `build` hard-errors rather than dropping it
  (`src/engine/preagg.rs:784-791`).

Do **not** hand-roll the predicate into `expr:` as a workaround. For a `sum`
that is strictly worse than declaring `filters:`. The `COALESCE` is keyed off
the measure *having* a `filters:` block — on the live path
(`src/engine/sql_generator.rs:2803-2812`) exactly as in the rollup — so a
hand-rolled `expr: CASE WHEN … END` gets no `COALESCE` on either tier: a group
where nothing matches comes back `NULL`, where the same measure written with
`filters:` returns `0`. It is consistent between tiers, and consistently not
what the `filters:` spelling means. The hand-rolled form is also invisible to
the parts of the system that know about filters — it is just an expr.

A measure's `filters:` are part of the rollup's definition fingerprint
(`src/engine/preagg.rs:129-147`), so adding, editing or removing one moves the
hash and invalidates the rollup — see
[Two gates outside `covers()`](#two-gates-outside-covers).

### Freshness (`refresh_key`)

`refresh_key` declares when `build` may skip a rollup that is still current
instead of rebuilding it. Without one, **every `build` rebuilds every rollup**
(`check_freshness` returns `is_fresh: false` when no key is configured,
`src/engine/preagg.rs:3449-3453`).

It takes exactly one of `sql:` or `every:` — any other key, both, or neither is
a parse error (`src/schema/models.rs:1350-1376`):

```yaml
pre_aggregations:
  - name: by_platform_daily
    dimensions: [platform]
    measures: [event_count, total_revenue]
    time_dimension: created_at
    granularity: day
    refresh_key:
      sql: "SELECT MAX(updated_at) FROM events"
      # or: every: "6h"
```

- **`sql:`** — `build` runs the query and reads the value of the **first
  selected column** (`src/cli/mod.rs:4308-4324`). The rollup is fresh when that
  value equals what the manifest recorded at the last build. If the SQL fails,
  `build` warns on stderr and rebuilds.
- **`every:`** — an interval of the form `<n><suffix>` with suffix `s`, `m`,
  `h`, `d` or `w` (`parse_interval`, `src/engine/preagg.rs:3390-3418`). The
  rollup is fresh while less than that has elapsed since the manifest's
  `refresh_key_checked_at`. No recorded timestamp means stale.

`refresh_key` may be declared per-rollup or view-wide (`View.refresh_key`); a
rollup's own key wins (`src/cli/mod.rs:4289-4293`). A freshness verdict is only
trusted against a previous manifest row with the **same** `rollup_hash`
(`src/cli/mod.rs:4300-4307`), so a definition edit forces a rebuild whatever the
refresh key says.

Skipped rollups are reported in `BuildPlan.skipped` and on stderr. The manifest
carries two extra columns for this, `refresh_key_value` and
`refresh_key_checked_at`; `build` adds them to an older `__manifest` via the
best-effort `BuildPlan.migrations` DDL. `--dry-run` does not evaluate
`refresh_key`.

## Config

Add a `pre_aggregations` section to your `config.yml`:

```yaml
databases:
  - name: warehouse
    type: duckdb
    path: ./data/warehouse.duckdb

pre_aggregations:
  schema: preagg          # schema/dataset name for rollup tables (default: AIRLAYER)
  database: warehouse     # which database to build into (default: first in config)
```

## CLI commands

### `airlayer build`

Creates rollup tables in the warehouse.

```bash
airlayer build --config config.yml                    # build all views
airlayer build --config config.yml --view events      # build one view
airlayer build --config config.yml --dry-run           # print SQL without executing
airlayer build --config config.yml --schema my_schema  # custom schema name
```

For each rollup, `build` creates:
- A schema (if it doesn't exist): `CREATE SCHEMA IF NOT EXISTS "preagg"`
- A manifest table: `"preagg"."__manifest"` tracking all rollups
- A rollup table via CTAS: `"preagg"."events__by_platform_daily__abc123__20260415"`

After all new tables are created and the manifest is updated, `build` automatically drops old rollup tables that were replaced. This prevents stale tables from accumulating across rebuilds. Cleanup only runs after the new table and manifest upsert succeed, so there is no downtime.

The same pass prunes **orphaned** rollups: a manifest row for a view in the build's scope whose rollup the view no longer declares (renamed, deleted, or a `default` rollup left over from a version before pre-aggregation became opt-in). Its table is dropped and its manifest row deleted — otherwise it would keep serving data frozen at its last build date that no `build` could refresh. Rollups skipped as fresh, and rollups belonging to views outside the build's scope (e.g. under `--view`), are never pruned. Pruned rollups are listed in `BuildPlan.pruned` and in the `pruned` array of `build`'s JSON summary.

The manifest stores metadata (view name, rollup name, hash, table name, dimensions, measures, time dimension, granularity, build date, and the two `refresh_key_value` / `refresh_key_checked_at` columns) so that `pull` and `query` can discover available rollups. `build` is the only command that migrates an older `__manifest`; every other read happens before or without that migration, which is why `manifest_query_sql` emits `SELECT *` rather than naming columns that may not exist yet.

### `airlayer pull`

Downloads rollup data from the warehouse to local Parquet files.

```bash
airlayer pull --config config.yml                     # pull all rollups
airlayer pull --config config.yml --view events       # pull one view
```

Pull reads the `__manifest` table, queries each rollup table, and writes the results to `.airlayer/cache/` as Parquet files alongside a `manifest.json` index.

### `airlayer query -x --no-cache`

Bypass all pre-aggregation layers:

```bash
airlayer query -x --config config.yml --no-cache \
  --dimension events.platform \
  --measure events.total_revenue
```

## Coverage resolution

A rollup answers a query only when `covers()` (`src/engine/preagg.rs:1989`)
returns true for it — reached from `check_coverage` (`:1294`) on the local
tier and directly from `resolve_warehouse` (`:3078`) on the warehouse tier.

`covers()` fails **closed**, and the reason matters: the caller hands the
rollup's rows back under the *raw* query's compiled SQL, with nothing in the
envelope to say which tier answered. A clause this check overlooked would not
surface as an unsupported feature — it would surface as a plausible wrong
number. So anything the re-aggregation cannot reproduce exactly sends the query
to the warehouse.

The checks run in the order below. Any one of them failing rejects that rollup;
the resolver then tries the next manifest entry, and falls through to raw SQL if
none match.

### 1. Hard disqualifiers

Checked first, before dimensions or measures are even looked at
(`src/engine/preagg.rs:2009-2020`):

| Request field | Why it disqualifies |
|---|---|
| non-empty `segments` | The re-aggregation SQL never emits segment predicates. Dropping one **widens** the result — the one direction a filter must never fail in. |
| any `motif` | The envelope advertises the motif's window columns (`z_score`, `growth_rate`, …) from the compiled raw query, and rollup rows carry none of them. |
| `ungrouped` | Asks for source rows; a rollup holds only aggregates. |
| `timezone` **and** a non-empty `time_dimensions` | The raw path converts the column before truncating it; the rollup's buckets were cut in the warehouse's own zone at build time. A timezone with no time dimension is fine — `compile_filter` never receives the timezone, so a filter on a time field means the same thing on both paths. |

### 2. Filters

Two conditions, both required (`src/engine/preagg.rs:2023-2059`):

1. **Every filter member is in the rollup.** Members are collected recursively
   through `and` / `or` trees, and each must be one of the rollup's
   `dimensions` or its `time_dimension`.
2. **Every filter actually renders.** `build_reagg_where_clause` collects with
   `filter_map`, so a filter this layer cannot express would silently vanish
   from the `WHERE` and the rollup would answer a *wider* question than was
   asked. `covers()` therefore calls `render_filter_sql` on each filter and
   refuses the rollup if any returns `None`.

Renderability is not universal. `render_filter_sql` (`:1609`) handles `equals`,
`notEquals`, `gt`, `gte`, `lt`, `lte`, `set`, `notSet`, `contains`,
`notContains`, `inDateRange` and `notInDateRange`. The other nine operators —
`startsWith`, `notStartsWith`, `endsWith`, `notEndsWith`, `beforeDate`,
`beforeOrOnDate`, `afterDate`, `afterOrOnDate`, `onTheDate` — are not rendered
at all and decline the rollup. So do these shapes:

- a comparison operator with no values (`IN ()` is a syntax error and `> NULL`
  is never true; either would answer wrongly rather than decline)
- `contains` / `notContains` on the rollup's declared time dimension — a
  substring match asks about text the bucket threw away
- `inDateRange` / `notInDateRange` with other than two values, or over a
  dimension that is not the rollup's time dimension
- a bound that does not line up with a stored bucket edge (see
  [Time dimensions](#6-time-dimensions))
- an `and` / `or` node where **any** child fails to render — dropping a
  conjunct widens the result and dropping a disjunct narrows it

Renderability is checked independently of source and dialect: escaping only
changes how a renderable value is written out, so `covers()` probes with
Postgres quoting and gets the same verdict either tier would.

### 3. Views

Every view referenced anywhere in the request must equal the rollup's single
`view_name` (`:2062-2067`). A rollup is built from one view with no joins, so a
cross-view query is never covered.

### 4. Dimensions

Every requested dimension must be one of the rollup's stored `dimensions`
(`:2070-2075`). There is no re-derivation: a dimension the rollup did not group
by cannot be recovered from its rows.

### 5. Measures

Every requested measure must be present in the rollup's stored measures, and
its type must be re-aggregable (`:2089-2100`). `custom`, `number` and `median`
are rejected here; `count_distinct` and `count_distinct_approx` are **accepted**
— see [Eligible measure types](#eligible-measure-types).

### 6. Time dimensions

For each requested time dimension (`:2103-2140`):

- Its name must equal the rollup's `time_dimension`.
- If the rollup has **no** `granularity`, no bucket column was ever built, so a
  request asking for one is refused. A bare `date_range` with no granularity is
  still servable, provided `dimensions:` stored the raw value.
- The requested granularity must be **coarser than or equal to** the stored one
  (`is_coarser_or_equal`, `:2145`), over the order `second, minute, hour, day,
  week, month, quarter, year`. Two exceptions:
  - **`week` → `month` / `quarter` / `year` is refused.** A week is not a whole
    number of months, so a week straddling a month boundary would be assigned
    entirely to the month of its start and the days on the far side would land
    in the wrong month. Before this was refused, such a query bound to the
    rollup and returned a plausible wrong number.
  - **anything *finer* → `week` is refused on the local Parquet tier**
    (`local_trunc`). `date_trunc('week', …)` is DuckDB's Monday whatever
    dialect built the rollup. A `week` rollup answering a `week` request is
    fine — no re-truncation happens at all when the grains match. On the
    warehouse tier the truncation uses the same `Dialect::date_trunc` that cut
    the buckets, so `day → week` is exact there too.
- A `date_range` must have exactly two bounds, and on a rollup with a
  granularity both must align to stored bucket edges (`date_range_bounds`,
  `:1499`). `gte '2026-01-15'` on a month rollup would otherwise hand back
  January from the 1st; an inclusive upper bound must cover its bucket to the
  end, so `2026-03-31` on a month rollup is fine and `2026-03-30` is refused
  rather than rounded up. No sub-day rollup can serve an inclusive upper bound
  at all, from either direction: a bare date lands on a bucket edge but names
  more buckets than one (refused by the span check, `:1564`), and an instant
  names no whole bucket (refused by the boundary check, `:1550`).
- **A `week`-granularity rollup cannot serve any query that compares its time
  dimension to a value** — no `date_range`, and no `equals` / `notEquals` /
  `gt` / `gte` / `lt` / `lte` on the bucket either, since all of them go
  through the same bound alignment. Where a week starts is a property of the
  dialect that built the rollup — Monday on most warehouses, Sunday on
  BigQuery, MySQL and Domo — and the manifest does not record which. Rather
  than shift the window by a day and drop or add a whole bucket at each edge,
  `week_start_is_ambiguous` (`:1514`) refuses the alignment outright. (`set` /
  `notSet` still render; they compare against nothing.)

### Re-aggregation

Once a rollup is chosen, the re-aggregation query reads it instead of the raw
table:

- `sum` columns are re-aggregated with `SUM()`
- `count` columns are re-aggregated with `SUM()` (summing pre-counted values)
- `average` is recomputed as `SUM(m__sum) / NULLIF(SUM(m__count), 0)`, with
  dialect-appropriate casting
- `min` / `max` are re-aggregated with `MIN()` / `MAX()`
- `count_distinct` and `count_distinct_approx` are re-aggregated with
  `COUNT(DISTINCT …)` over the stored raw column
- a coarser time bucket is re-truncated with the dialect's `DATE_TRUNC`

When the request's grouping key is **exactly** the rollup's stored grain,
`matches_exact_grain` (`:2197`) drops the `GROUP BY` and the per-measure
aggregate wrapper entirely — the rollup already holds one row per requested
group, so re-aggregating is a no-op, and `GROUP BY` is a blocking operator that
would stop the planner honouring a `LIMIT` cheaply. A rollup storing a
`count_distinct`, `count_distinct_approx` or `median` measure never qualifies:
its real on-disk grain is finer than its declared `dimensions`.

### Two gates outside `covers()`

Coverage is necessary, not sufficient. Two further checks sit around it.

**Staleness.** `is_live` (`:1288`) requires the entry's `(view_name,
rollup_hash)` to still be declared by the current schema. The hash is a
fingerprint of the rollup's *definition* as well as its shape.
`definition_fingerprint` (`:94-150`) covers the view name, its `table:` /
`sql:`, each dimension's name and `expr`, and each measure's name, type, `expr`
and `filters:`; `compute_rollup_hash` (`:35-61`) then mixes that together with
the rollup's dimension set, measure names, time dimension and **granularity**.
So editing
`expr: amount` to `expr: amount - refunds`, flipping a `type:`, adding a measure
`filters:` entry or repointing `table:` all move the hash, and the read path
then declines the old entry instead of answering with pre-edit numbers. Build a
`LiveRollups` set with `preagg::live_rollups(&views)` and pass it to whichever
resolver you call; passing `None` keeps the old name-only matching. That choice
is reported back as `stale_checked: false` only by `resolve_cached` and the
WASM/FFI entry points — `resolve_local` and `resolve_warehouse` return a
`PreaggResolution`, which carries no such flag, so a Rust embedder gets no
signal and has to pass the set deliberately. A declined entry is not evicted —
prune stored blobs against `live_rollup_keys`.

**Freshness.** Separate from staleness, and it lives outside this repo. The
airlayer side only records `refresh_key_value` / `refresh_key_checked_at` in the
manifest. In oxy (oxygen-internal), `crates/agentic/semantic/src/compile.rs`
checks that recorded state after `check_coverage` returns and may decline a
covered rollup: read surfaces pass `RollupFreshness::ServeStale` (serve it and
seed a background rebuild — the **Pre-aggregated** badge says what it is), while
the anomaly scan that writes to the Insights Inbox passes
`RollupFreshness::RequireFresh` and falls through to the warehouse, because
there the number becomes an assertion about the data that can page Slack. That
gate is a cold-cache guard rather than a lag detector; consult that repo for its
current semantics rather than assuming from here.

## Dialect support

Pre-aggregation generates dialect-aware SQL for all 11 supported databases:

| Dialect | Identifier quoting | Date truncation | Schema DDL |
|---------|-------------------|-----------------|------------|
| Postgres | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| MySQL | `` `col` `` | `DATE_FORMAT(col, '%Y-%m-01')` | `CREATE SCHEMA IF NOT EXISTS` |
| BigQuery | `` `col` `` | `TIMESTAMP_TRUNC(col, MONTH)` | None (datasets created externally) |
| Snowflake | `"COL"` (uppercase) | `DATE_TRUNC('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| DuckDB | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| ClickHouse | `"col"` | `toStartOfMonth(col)` | `CREATE DATABASE IF NOT EXISTS` |
| Databricks | `` `col` `` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| Redshift | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| SQLite | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| Domo | `` `col` `` | `DATE_FORMAT(col, '%Y-%m-01')` | `CREATE SCHEMA IF NOT EXISTS` |
| Presto | `"col"` | `DATE_TRUNC('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |

### ClickHouse specifics

- Uses `ReplacingMergeTree(build_date)` for the manifest table, keyed
  `ORDER BY (view_name, rollup_name)` (`src/engine/preagg.rs:1058-1059`) —
  **not** by `rollup_hash`. The rollup's own name is not part of the hash, so
  deduplicating on the hash alone would collapse two identically-shaped rollups
  of one view into a single manifest row. Note the consequence for
  [staleness](#two-gates-outside-covers): a definition edit moves the hash but
  the new row still replaces the old one here, because the key did not change.
- Uses `MergeTree` for rollup tables, with `SETTINGS allow_nullable_key = 1` on any
  keyed rollup — a rollup's `ORDER BY` *is* the sorting key, and a nullable grouping
  column there is otherwise rejected at DDL time (`Code: 44 ILLEGAL_COLUMN`)
- Manifest upsert is a plain `INSERT INTO` (ReplacingMergeTree handles dedup)
- Queries use `FINAL` when reading the manifest to get deduplicated results

### BigQuery specifics

- No `PRIMARY KEY` in DDL
- Uses `FLOAT64` for double-precision casts
- Uses `STRING` type for text columns in manifest
- Datasets must be created externally before building

### Snowflake specifics

- All identifiers are uppercased when quoted (e.g., `"PLATFORM"`, `"__MANIFEST"`)

## Library API

All pre-aggregation logic is available as pure functions in `airlayer::engine::preagg` for use as a library (e.g., from oxy-internal). These functions perform no I/O — the caller handles database execution.

### Query resolution

Each resolver takes an `Option<&LiveRollups>`. Pass `Some(..)` — built from the
views you are compiling against — and a rollup whose definition has been edited
since it was built is declined instead of answering with pre-edit numbers.
Passing `None` matches on member names alone and is the unprotected behaviour.

```rust
use airlayer::engine::preagg::{self, PreaggResolution};

// The set of (view_name, rollup_hash) pairs the current schema declares.
let live = preagg::live_rollups(&views);

// Layer 1: local Parquet cache
if let Some(PreaggResolution::LocalParquet { reagg_sql, parquet_path }) =
    preagg::resolve_local(&request, &local_manifest, &cache_dir, Some(&live))
{
    // Execute reagg_sql against in-memory DuckDB
}

// Layer 2: warehouse rollup tables
let manifest_sql = preagg::manifest_query_sql(&schema, &dialect);
// ... execute manifest_sql, get rows ...
let entries = preagg::parse_manifest_rows(&rows);
if let Some(PreaggResolution::WarehouseRollup { reagg_sql, table_name }) =
    preagg::resolve_warehouse(&request, &entries, &schema, &dialect, Some(&live))
{
    // Execute reagg_sql against the warehouse
}
```

`manifest_query_sql` deliberately emits `SELECT *` rather than a column list: on
a deployment whose `__manifest` predates the `refresh_key_*` columns, naming
them would error the whole query and take `pull` down outright.

### Build planning

```rust
use airlayer::engine::preagg;

let plan = preagg::collect_build_sql(
    &views,
    &schema,
    &date_str,
    &dialect,
    previous_entries,  // Option<&[WarehouseRollupEntry]> — enables pruning
    freshness,         // Option<&[RollupFreshness]>     — enables refresh_key skips
)?;

// Order matters: the prelude creates the schema and __manifest, the
// migrations ALTER that table, then the rest builds the rollups.
for stmt in plan.statements.iter().take(plan.prelude_len) { /* execute */ }
for stmt in &plan.migrations { /* execute, ignoring failures */ }
for stmt in plan.statements.iter().skip(plan.prelude_len) { /* execute */ }

// plan.manifest_entries contains metadata for reporting;
// plan.skipped and plan.pruned say what was not rebuilt and what was removed.
```

`migrations` are best-effort DDL bringing an older `__manifest` up to the
current column set. They are expected to fail on a manifest that already has
the columns, and not every dialect can express `ADD COLUMN IF NOT EXISTS`, so
run them ignoring errors — and only after the `CREATE` that makes the table
exist.

### Key types

| Type | Description |
|------|-------------|
| `PreaggResolution` | Enum: `LocalParquet { reagg_sql, parquet_path }` or `WarehouseRollup { reagg_sql, table_name }` |
| `CachedResolution` | Struct: `reagg_sql` (FROM `"__cache"`), `cache_key`, `entry` — filesystem-independent variant |
| `WarehouseRollupEntry` | A rollup entry from the warehouse `__manifest` table |
| `BuildPlan` | `statements` + `manifest_entries` for a build, plus `migrations` (best-effort `__manifest` DDL), `prelude_len` (how many leading statements create the schema and manifest), `skipped` (still fresh per `refresh_key`) and `pruned` (no longer declared) |
| `LocalManifest` | The local `manifest.json` structure (from `pull`) |
| `LiveRollups` | `HashSet<(view_name, rollup_hash)>` of what the schema declares now; built with `live_rollups(&views)` and passed to every resolver |
| `RollupFreshness` | Per-rollup verdict fed to `collect_build_sql` so `refresh_key` can skip a rebuild. Unrelated to oxy's enum of the same name, which is a read-time gate |
| `FreshnessCheck` | What `check_freshness` returns: `is_fresh` plus the value to write back into the manifest |

## WASM / Browser cache API

The WASM module exposes pre-aggregation cache functions for browser use. The Rust side handles pure computation (coverage checking, SQL generation); the JavaScript caller handles I/O (IndexedDB storage, duckdb-wasm execution).

### Functions

| Function | Description |
|----------|-------------|
| `cache_resolve(manifest_json, query_json, views_yaml?)` | Check if a cached rollup covers a query. Returns `{ reagg_sql, cache_key, entry, stale_checked }` or `null`. |
| `cache_build_manifest(rows_json, source_database)` | Parse warehouse manifest rows into a `LocalManifest` JSON string for IndexedDB storage. |
| `cache_key(view_name, rollup_hash)` | Get the IndexedDB key for a rollup (e.g., `"events__a1b2c3d4"`). |
| `cache_resolve_warehouse(rows_json, query_json, schema, dialect, views_yaml?)` | Resolve against warehouse rollup entries. Returns `{ reagg_sql, table_name, stale_checked }` or `null`. |
| `cache_live_keys(views_yaml)` | The cache keys the current schema declares — the IndexedDB retain-set. |

### Typical browser flow

```javascript
import init, { cache_build_manifest, cache_resolve, cache_key } from './airlayer_bg.wasm';

// 1. Fetch manifest rows from warehouse and build local manifest
const manifestJson = cache_build_manifest(JSON.stringify(warehouseRows), "my_warehouse");

// 2. Store manifest + rollup data in IndexedDB
await idb.put('manifest', manifestJson);
for (const entry of warehouseRows) {
  const key = cache_key(entry.view_name, entry.rollup_hash);
  const data = await fetchRollupData(entry.table_name);
  await idb.put(key, data);
}

// 3. On query, check cache coverage.
//    Pass the views: a rollup's hash covers its members' definitions, so an
//    edited `expr:` or `type:` moves it and a manifest row the schema no longer
//    declares is declined instead of answering with pre-edit numbers. Omit them
//    and the match is on member names alone — `stale_checked` reports which.
const resolution = cache_resolve(manifestJson, JSON.stringify(query), viewsYaml);
if (resolution) {
  // Load cached data into duckdb-wasm table named "__cache"
  const data = await idb.get(resolution.cache_key);
  await duckdb.exec(`CREATE TABLE "__cache" AS SELECT * FROM '${dataUrl}'`);
  const result = await duckdb.exec(resolution.reagg_sql);
  await duckdb.exec('DROP TABLE "__cache"');
}
```

The `reagg_sql` reads from a table named `"__cache"` — the JS caller must create this table in duckdb-wasm with the cached rollup data before executing.

### Evicting stale blobs

Declining a stale manifest row stops it being *read*; it does not delete the IndexedDB blob stored under the old key, and once the row is gone nothing can name that key again. Prune with the schema's retain-set:

```javascript
import { cache_live_keys } from './airlayer_bg.wasm';

const live = new Set(cache_live_keys(viewsYaml));
for (const key of await idb.keys()) {
  if (key !== 'manifest' && !live.has(key)) await idb.del(key);
}
```

The same argument exists on the FFI side (`airlayer_cache_resolve`, `airlayer_cache_resolve_warehouse` take an optional `views` array; `airlayer_cache_live_keys` returns the retain-set).

## Example

The `examples/pre-aggregation/` directory contains a complete working demo with a 500M-row DuckDB database:

```bash
cd examples/pre-aggregation
./demo.sh     # seeds data, builds, pulls, and queries
```

The demo shows the speedup from scanning ~1,000 cached rows instead of 500,000,000 raw rows.
