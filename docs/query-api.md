# Query API

## QueryRequest

Queries are represented as JSON objects:

```json
{
  "dimensions": ["orders.status", "orders.order_date"],
  "measures": ["orders.total_revenue", "orders.count"],
  "filters": [
    {
      "member": "orders.status",
      "operator": "equals",
      "values": ["active"]
    }
  ],
  "segments": ["orders.active_only"],
  "time_dimensions": [
    {
      "dimension": "orders.order_date",
      "granularity": "month",
      "date_range": ["2024-01-01", "2024-12-31"]
    }
  ],
  "order": [
    {"id": "orders.total_revenue", "desc": true}
  ],
  "limit": 100,
  "offset": 0,
  "timezone": "America/New_York",
  "ungrouped": false
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dimensions` | `string[]` | No | Dimension members to select (`view.dimension`) |
| `measures` | `string[]` | No | Measure members to select (`view.measure`) |
| `filters` | `Filter[]` | No | Filter conditions |
| `segments` | `string[]` | No | Named segments to apply |
| `time_dimensions` | `TimeDimension[]` | No | Time dimensions with granularity |
| `order` | `OrderBy[]` | No | Sort order |
| `limit` | `number` | No | Row limit |
| `offset` | `number` | No | Row offset |
| `timezone` | `string` | No | IANA timezone for time dimensions |
| `ungrouped` | `boolean` | No | If true, skip GROUP BY |

At least one dimension or measure must be specified.

## Filters

### Simple filter

```json
{
  "member": "orders.status",
  "operator": "equals",
  "values": ["active"]
}
```

### Nested filters (AND/OR)

```json
{
  "or": [
    {
      "member": "orders.status",
      "operator": "equals",
      "values": ["active"]
    },
    {
      "and": [
        {
          "member": "orders.amount",
          "operator": "gt",
          "values": ["1000"]
        },
        {
          "member": "orders.status",
          "operator": "equals",
          "values": ["pending"]
        }
      ]
    }
  ]
}
```

### Filter operators

| Operator | Description | Values required |
|----------|-------------|----------------|
| `equals` | Exact match (single value: `=`, multiple: `IN`) | Yes |
| `notEquals` | Not equal / NOT IN | Yes |
| `contains` | `LIKE '%value%'` | Yes |
| `notContains` | `NOT LIKE '%value%'` | Yes |
| `startsWith` | `LIKE 'value%'` | Yes |
| `notStartsWith` | `NOT LIKE 'value%'` | Yes |
| `endsWith` | `LIKE '%value'` | Yes |
| `notEndsWith` | `NOT LIKE '%value'` | Yes |
| `gt` | Greater than | Yes |
| `gte` | Greater than or equal | Yes |
| `lt` | Less than | Yes |
| `lte` | Less than or equal | Yes |
| `set` | IS NOT NULL | No |
| `notSet` | IS NULL | No |
| `inDateRange` | Between two dates | Yes (2 values) |
| `notInDateRange` | Not between two dates | Yes (2 values) |
| `beforeDate` | Before a date | Yes |
| `beforeOrOnDate` | Before or on a date | Yes |
| `afterDate` | After a date | Yes |
| `afterOrOnDate` | After or on a date | Yes |

### Filter routing

- Filters on **dimensions** are placed in the `WHERE` clause
- Filters on **measures** are placed in the `HAVING` clause
- This routing is automatic based on member type

## Time dimensions

```json
{
  "dimension": "orders.order_date",
  "granularity": "month",
  "date_range": ["2024-01-01", "2024-12-31"]
}
```

Supported granularities: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`.

The granularity expression is dialect-specific (see [dialects.md](dialects.md)).

Date ranges add WHERE conditions: `dimension >= start AND dimension < end`.

## QueryResult

```rust
pub struct QueryResult {
    pub sql: String,           // Generated SQL
    pub params: Vec<String>,   // Parameter values (in order)
    pub columns: Vec<ColumnMeta>,  // Column metadata
}

pub struct ColumnMeta {
    pub key: String,           // e.g., "orders__status"
    pub member_type: String,   // "dimension" or "measure"
    pub member_name: String,   // e.g., "orders.status"
}
```

## Library usage

```rust
use airlayer::{SemanticEngine, DatasourceDialectMap, Dialect};
use airlayer::engine::query::QueryRequest;

let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
let engine = SemanticEngine::load(
    Path::new("."),   // base directory containing .view.yml files
    None,             // optional globals file
    dialects,
)?;

let request: QueryRequest = serde_json::from_str(r#"{
    "dimensions": ["orders.status"],
    "measures": ["orders.total_revenue"]
}"#)?;

let result = engine.compile_query(&request)?;
println!("{}", result.sql);
println!("params: {:?}", result.params);
```

## CLI usage

### Shorthand flags

```bash
# Dialect inferred from view files
airlayer query \
  --dimension orders.status \
  --measure orders.total_revenue \
  --filter orders.status:equals:active \
  --order orders.total_revenue:desc \
  --limit 10

# Or with Oxy config.yml
airlayer query -c config.yml \
  --dimension orders.status \
  --measure orders.total_revenue
```

### JSON query

```bash
airlayer query -q '{
  "dimensions": ["orders.status"],
  "measures": ["orders.total_revenue"],
  "filters": [{"member": "orders.status", "operator": "equals", "values": ["active"]}]
}'
```

### Stdin

```bash
echo '{"dimensions": ["orders.status"]}' | airlayer query -q -
```

## Motifs

Motifs add post-aggregation analytical columns by wrapping the base query as a CTE. Use `--motif <name>` on the CLI or `"motif": "<name>"` in the JSON query.

### CLI usage

```bash
# Non-time motif
airlayer query --execute --config config.yml \
  --dimension orders.category \
  --measure orders.total_revenue \
  --motif contribution

# Time-series motif (requires JSON for time_dimensions)
airlayer query --execute --config config.yml -q '{
  "measures": ["orders.total_revenue"],
  "time_dimensions": [{"dimension": "orders.created_at", "granularity": "month"}],
  "motif": "mom"
}'
```

### Builtin motifs

| Motif | Output columns | Requires time dim |
|-------|---------------|-------------------|
| `yoy`, `qoq`, `mom`, `wow`, `dod` | `previous_value`, `growth_rate` | Yes |
| `anomaly` | `mean_value`, `stddev_value`, `z_score`, `is_anomaly` | No |
| `contribution` | `total`, `share` | No |
| `trend` | `row_n`, `slope`, `intercept`, `trend_value` | Yes |
| `moving_average` | `moving_avg` | Yes |
| `rank` | `rank` | No |
| `percent_of_total` | `percent_of_total` | No |
| `cumulative` | `cumulative_value` | Yes |

### Multi-measure expansion

When a query has multiple measures, motif columns are emitted per-measure with `{measure_short}__{motif_col}` naming:

```bash
# Two measures → total_revenue__share, order_count__share, etc.
airlayer query --execute --config config.yml \
  --dimension orders.category \
  --measure orders.total_revenue orders.order_count \
  --motif contribution
```

### Motif parameters

Some motifs accept parameters via the `motif_params` field in JSON queries:

```json
{
  "measures": ["orders.total_revenue"],
  "motif": "anomaly",
  "motif_params": {"threshold": 3}
}
```

| Param | Default | Used by |
|-------|---------|---------|
| `threshold` | `2` | `anomaly` (z-score threshold) |
| `window` | `6` | `moving_average` (ROWS PRECEDING, so 7-period window) |

### CTE architecture

- **Single-stage** (most motifs): `WITH __base AS (<sql>) SELECT b.*, <outputs> FROM __base b`
- **Two-stage** (anomaly, trend): intermediate CTE computes window functions, final stage references materialized columns

### Custom motifs

Custom motifs are loaded from `.motif.yml` files in the `motifs/` directory. See [schema-format.md](schema-format.md#motif-files-motifyml) for the file format.
