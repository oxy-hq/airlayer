# Bootstrapping a semantic layer

This example demonstrates the full workflow for bootstrapping a semantic layer from a database, using airlayer's agent-facing tools.

## The data

`data/orders.csv` — 12 rows of sales order data with customers, products, quantities, prices, discounts, and fulfillment status. DuckDB loads it automatically via `file_search_path` in the config.

## The workflow

### 1. Introspect the database schema

```bash
airlayer inspect --schema --config config.yml
```

Returns structured JSON with every table, column, and data type in the database. This is the starting point — the agent uses this to understand what's available before writing any `.view.yml` files.

Example output:
```json
{
  "database_type": "duckdb",
  "tables": [
    {
      "schema": "main",
      "name": "orders",
      "columns": [
        {"name": "order_id", "type": "VARCHAR", "nullable": true, "ordinal": 1},
        {"name": "customer_id", "type": "VARCHAR", "nullable": true, "ordinal": 2},
        {"name": "quantity", "type": "BIGINT", "nullable": true, "ordinal": 7},
        {"name": "unit_price", "type": "DOUBLE", "nullable": true, "ordinal": 8},
        ...
      ]
    }
  ]
}
```

### 2. Generate the view file

From the schema, the agent creates `views/orders.view.yml`:
- String/date columns → dimensions
- Numeric columns → measures with appropriate aggregation (sum, average, count_distinct)
- Computed measures for business logic (e.g., `revenue = quantity * unit_price * (1 - discount_pct)`)
- Entity declarations for joinable keys

### 3. Profile dimensions

```bash
airlayer inspect --profile orders --config config.yml
```

Runs type-aware profiling against the actual data:
- String dimensions: cardinality, distinct values, top values by frequency
- Number dimensions: min, max, mean
- Date dimensions: earliest, latest

This tells the agent what filter values are valid and what the data distribution looks like — without hardcoding enums.

### 4. Compile and verify

```bash
# Compile only — see the generated SQL
airlayer query --config config.yml \
  --dimension orders.category --measure orders.total_revenue

# Compile + execute — get the structured envelope
airlayer query --execute --config config.yml \
  --dimension orders.category --measure orders.total_revenue
```

The execution envelope includes `status`, `sql`, `data`, and `views_used` — everything the agent needs to verify correctness and iterate.

### 5. Iterate

If the results are wrong, the agent edits the `.view.yml` and re-runs. The semantic layer is both the input and output of the agent's work. The agent never writes SQL — it writes dimensions, measures, and expressions.

## Running the scripts

```bash
# Make sure airlayer is built with executor support
cargo build --features exec

# Run each step
./01_introspect_schema.sh
./02_profile_dimensions.sh
./03_compile_query.sh
./04_execute_query.sh
```
