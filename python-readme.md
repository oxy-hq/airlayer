# airlayer

An in-process semantic engine that compiles `.view.yml` definitions into dialect-specific SQL. Define your metrics once — dimensions, measures, filters, segments — and compile to Postgres, BigQuery, Snowflake, DuckDB, and 6 other dialects.

```python
import airlayer

result = airlayer.compile(
    views_yaml=[open("orders.view.yml").read()],
    query_json='{"measures": ["orders.total_revenue"], "dimensions": ["orders.status"]}',
    dialect="postgres",
)
print(result["sql"])
print(result["columns"])
```

No database connection required. No server to deploy. The compiler runs in-process — pass in YAML definitions and a query, get back SQL, bind parameters, and column metadata.

## Features

- **10 SQL dialects**: Postgres, BigQuery, Snowflake, DuckDB, MySQL, ClickHouse, Redshift, Databricks, SQLite, Domo
- **Automatic joins**: Declare entities on views, and airlayer infers JOINs via BFS on the entity graph
- **12 built-in motifs**: contribution, rank, anomaly detection, YoY/MoM/WoW, trend, moving average, cumulative, and more
- **Custom motifs**: Define reusable analytical patterns in `.motif.yml` files
- **Validation**: Check `.view.yml` files for errors without compiling a query

## API

**`airlayer.compile(views_yaml, query_json, dialect, ...)`** → `dict` with `sql`, `params`, `columns`

**`airlayer.validate(views_yaml, ...)`** → `True` or raises `ValueError`

See the [full documentation](https://github.com/oxy-hq/airlayer) for the query format, schema reference, and library usage guide.
