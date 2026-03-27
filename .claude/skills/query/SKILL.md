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
  [--segments <view>.<segment>]
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
