---
name: query
description: Compile a semantic query to SQL using o3. Use when the user wants to generate SQL from .view.yml schemas, test a query against their semantic layer, or translate dimensions/measures/filters into dialect-specific SQL.
allowed-tools:
  - Bash
  - Read
  - Glob
  - Grep
argument-hint: "[--dimensions view.dim --measures view.measure --filter view.dim:operator:value]"
---

# o3 Query — Compile Semantic Queries to SQL

You are compiling a semantic layer query to SQL using the `o3` CLI.

## Prerequisites

Ensure `o3` is installed:
```bash
which o3 || cargo install --git https://github.com/oxy-hq/o3
```

## Locate the semantic layer

1. Find `.view.yml` files in the project. They are typically under a `views/` or `semantics/views/` directory.
2. Identify the base directory (parent of `views/`).

```bash
find . -name "*.view.yml" -not -path "*/node_modules/*" -not -path "*/cube/*" 2>/dev/null | head -20
```

## Inspect available members

Before building a query, inspect what's available:

```bash
o3 inspect --path <base_dir>
```

This lists all views, dimensions, measures, and entities.

## Build and run the query

Use CLI flags (preferred for LLM tool use):

```bash
o3 query --path <base_dir> -d <dialect> \
  --dimensions <view.dimension> \
  --measures <view.measure> \
  --filter <view.dimension>:<operator>:<value> \
  --order <view.member>:desc \
  --limit 100
```

Or JSON input for complex queries:

```bash
o3 query --path <base_dir> -d <dialect> -q '{
  "dimensions": ["view.dimension"],
  "measures": ["view.measure"],
  "filters": [{"member": "view.dim", "operator": "equals", "values": ["val"]}],
  "order": [{"id": "view.measure", "desc": true}],
  "limit": 100
}'
```

## Arguments from user

$ARGUMENTS

## Dialect selection

Pick the dialect based on the project's database:
- `postgres` (default), `mysql`, `bigquery`, `snowflake`, `duckdb`, `clickhouse`, `databricks`, `redshift`, `sqlite`, `domo`

If the project has a `config.yml` with database definitions, use `-c config.yml` instead of `-d`.

## Filter operators

`equals`, `notEquals`, `contains`, `notContains`, `startsWith`, `notStartsWith`, `endsWith`, `notEndsWith`, `gt`, `gte`, `lt`, `lte`, `set`, `notSet`, `inDateRange`, `notInDateRange`, `beforeDate`, `beforeOrOnDate`, `afterDate`, `afterOrOnDate`, `onTheDate`

## Advanced features

- **Segments**: `--segments view.segment_name` (predefined filter conditions)
- **Join hints**: `--through entity_name` (disambiguate multi-path joins)
- **Time dimensions** (JSON only): `"time_dimensions": [{"dimension": "view.date_col", "granularity": "month", "date_range": ["2024-01-01", "2024-12-31"]}]`
- **Relative dates** (JSON only): `"date_range": ["last 7 days"]` or `["this month"]`
- **Ungrouped mode** (JSON only): `"ungrouped": true` for raw rows without aggregation

## Output

o3 prints the generated SQL to stdout and params to stderr. Show the SQL to the user.
