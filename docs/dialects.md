# SQL Dialects

o3 supports 10 SQL dialects. Each dialect customizes identifier quoting, date truncation, timezone conversion, parameter placeholders, and type casting.

## Supported dialects

| Dialect | Aliases | Identifier quoting | Param placeholders | Date truncation |
|---------|---------|-------------------|-------------------|-----------------|
| Postgres | `postgres`, `postgresql`, `pg` | `"col"` | `$1, $2, ...` | `date_trunc('month', x)` |
| MySQL | `mysql` | `` `col` `` | `?` | `DATE_FORMAT(x, '%Y-%m-01')` |
| BigQuery | `bigquery`, `bq` | `` `col` `` | `@p0, @p1, ...` | `TIMESTAMP_TRUNC(x, MONTH)` |
| Snowflake | `snowflake`, `sf` | `"col"` | `?` | `DATE_TRUNC('month', x)` |
| DuckDB | `duckdb`, `duck` | `"col"` | `$1, $2, ...` | `date_trunc('month', x)` |
| ClickHouse | `clickhouse`, `ch` | `"col"` | `$1, $2, ...` | `toStartOfMonth(x)` |
| Databricks | `databricks` | `` `col` `` | `?` | `date_trunc('month', x)` |
| Redshift | `redshift`, `rs` | `"col"` | `$1, $2, ...` | `date_trunc('month', x)` |
| SQLite | `sqlite` | `"col"` | `?` | `date_trunc('month', x)` |
| Domo | `domo` | `` `col` `` | `?` | `DATE_FORMAT(x, '%Y-%m-01')` |

## Dialect resolution

Dialect is resolved in order of precedence:

1. **`-d` flag** — explicit dialect override, applied to all views
2. **`-c config.yml`** — each view's `datasource` field maps to a database entry with a `type` field (Oxy projects)
3. **View-level `dialect` field** — declared directly in `.view.yml` (standalone projects)
4. **Default** — falls back to Postgres

For standalone projects, declare `dialect:` in each view file. For Oxy projects, use `datasource:` + `config.yml`.

```yaml
# config.yml
databases:
  - name: warehouse
    type: bigquery
  - name: operational
    type: postgres
  - name: analytics
    type: domo
    dataset_id: 779b5f00-9557-4ecb-b7b2-d656932a63c7
    developer_token_var: DOMO_DEVELOPER_TOKEN
    instance: mycompany
```

## Per-dialect behavior

### Timezone conversion

| Dialect | Expression |
|---------|-----------|
| Postgres/Redshift | `(x::timestamptz AT TIME ZONE 'tz')` |
| MySQL | `CONVERT_TZ(x, 'UTC', 'tz')` |
| BigQuery | `DATETIME(x, 'tz')` |
| Snowflake | `CONVERT_TIMEZONE('UTC', 'tz', x::TIMESTAMP_NTZ)` |
| DuckDB | `timezone('tz', x::TIMESTAMPTZ)` |
| ClickHouse | `toTimeZone(x, 'tz')` |
| Databricks | `from_utc_timestamp(x, 'tz')` |
| SQLite/Domo | Not supported (expression returned as-is) |

### Timestamp casting

| Dialect | Expression |
|---------|-----------|
| Postgres/Redshift | `x::timestamptz` |
| MySQL/BigQuery | `TIMESTAMP(x)` |
| Snowflake | `TO_TIMESTAMP(x)` |
| DuckDB | `x::TIMESTAMP` |
| ClickHouse | `toDateTime(x)` |
| Databricks/Domo | `CAST(x AS TIMESTAMP)` |
| SQLite | passthrough |

### Count distinct approximation

| Dialect | Expression |
|---------|-----------|
| BigQuery/Snowflake/Databricks | `APPROX_COUNT_DISTINCT(x)` |
| ClickHouse | `uniqHLL12(x)` |
| Redshift | `APPROXIMATE COUNT(DISTINCT x)` |
| Others | `COUNT(DISTINCT x)` (exact) |

## Domo-specific notes

Domo's SQL engine is MySQL-based. Key considerations:

- **Table names are dataset UUIDs** — quoted with backticks: `` `779b5f00-9557-4ecb-b7b2-d656932a63c7` ``
- **Column names with spaces** use double quotes in expressions: `'"Video Views"'`
- **No timezone support** — timezone conversion is a no-op
- **MySQL-style date functions** — uses `DATE_FORMAT` rather than `DATE_TRUNC`
- **Results capped at 1M rows** by the Domo API
