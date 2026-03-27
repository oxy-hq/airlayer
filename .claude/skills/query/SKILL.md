---
name: query
description: Run a semantic query against the database via airlayer. Use when the user wants to query data through the semantic layer, test view definitions, or debug query results.
---

# Run a Semantic Query

You are running a semantic query through airlayer's execution interface.

## Usage

```bash
# Compile + execute (returns structured JSON envelope)
airlayer query --execute --config <config.yml> --path <views_dir> \
  --dimensions <view>.<dim> \
  --measures <view>.<measure> \
  [--filter <view>.<dim>:<operator>:<value>] \
  [--order <view>.<member>:asc|desc] \
  [--limit N] \
  [--segments <view>.<segment>] \
  [--motif <motif_name>]
```

## Filter operators

Format: `member:operator:value` (multiple values comma-separated)

| Operator | Example | Description |
|----------|---------|-------------|
| equals | `orders.status:equals:completed` | Exact match |
| notEquals | `orders.status:notEquals:cancelled` | Not equal |
| contains | `orders.name:contains:widget` | String contains |
| notContains | `orders.name:notContains:test` | String doesn't contain |
| startsWith | `orders.name:startsWith:Pro` | String starts with |
| endsWith | `orders.name:endsWith:Plan` | String ends with |
| gt | `orders.amount:gt:100` | Greater than |
| gte | `orders.amount:gte:100` | Greater than or equal |
| lt | `orders.amount:lt:1000` | Less than |
| lte | `orders.amount:lte:1000` | Less than or equal |
| in | `orders.status:in:completed,shipped` | In list |
| notIn | `orders.status:notIn:cancelled,returned` | Not in list |
| set | `orders.email:set` | Is not null |
| notSet | `orders.email:notSet` | Is null |
| beforeDate | `orders.created_at:beforeDate:2025-01-01` | Before date |
| afterDate | `orders.created_at:afterDate:2025-01-01` | After date |

## Reading the envelope

The `--execute` flag returns a JSON envelope:

```json
{
  "status": "success",           // or "parse_error", "compile_error", "execution_error"
  "sql": "SELECT ...",           // the compiled SQL
  "columns": [...],              // column metadata (name, member, kind)
  "data": [...],                 // result rows (max 50)
  "row_count": 3,                // true total row count
  "views_used": ["orders"],      // which .view.yml files were involved
  "error": null                  // error message if status != "success"
}
```

## Interpreting errors

- **parse_error**: Bad YAML in view files, or invalid query input. Fix the YAML syntax.
- **compile_error**: Member path doesn't exist, or join can't be resolved. Check dimension/measure names.
- **execution_error**: Database rejected the SQL. Check `expr` fields — the column names may be wrong. The `sql` field shows exactly what was sent.

## Motifs

Motifs add post-aggregation analytical columns by wrapping the base query as a CTE. Use `--motif <name>` on the CLI or `"motif": "<name>"` in JSON queries.

**Builtin motifs:** yoy, qoq, mom, wow, dod, anomaly, contribution, trend, moving_average, rank, percent_of_total, cumulative.

- **contribution**: adds `total` and `share` columns (what % does each group contribute?)
- **rank**: adds `rank` column (ordered by the measure descending)
- **percent_of_total**: adds `percent_of_total` column (100 * measure / total)
- **anomaly**: adds `mean_value`, `stddev_value`, `z_score`, `is_anomaly` columns
- **yoy/qoq/mom/wow/dod**: adds `previous_value` and `growth_rate` columns (requires a time dimension)
- **moving_average**: adds `moving_avg` column (7-period rolling average, requires time dimension)
- **cumulative**: adds `cumulative_value` column (running sum, requires time dimension)
- **trend**: adds `row_n`, `slope`, `intercept`, `trend_value` columns (linear regression, requires time dimension)

```bash
# Non-time motif (contribution analysis)
airlayer query --execute --config config.yml --path . \
  --dimensions orders.category \
  --measures orders.total_revenue \
  --motif contribution

# Time-series motif (day-over-day) — requires JSON for time_dimensions
airlayer query --execute --config config.yml --path . -q '{
  "measures": ["orders.total_revenue"],
  "time_dimensions": [{"dimension": "orders.order_date", "granularity": "day"}],
  "motif": "dod"
}'
```

## JSON query format

For complex queries, use `-q` with JSON:

```bash
airlayer query --execute --config config.yml --path . -q '{
  "dimensions": ["orders.category", "orders.region"],
  "measures": ["orders.total_revenue"],
  "filters": [{"member": "orders.status", "operator": "equals", "values": ["completed"]}],
  "order": [{"id": "orders.total_revenue", "desc": true}],
  "limit": 10
}'
```
