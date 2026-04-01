---
name: profile
description: Profile dimensions in the semantic layer to discover data values, ranges, and cardinality. Use when the user wants to understand what data is in a dimension, find valid filter values, or validate view definitions against actual data.
---

# Profile Dimensions

You are running type-aware data profiling through airlayer to discover what's in the database.

## Usage

```bash
# Profile all dimensions in a view
airlayer inspect --profile <view_name> --config <config.yml>
# Profile a single dimension
airlayer inspect --profile <view_name>.<dimension_name> --config <config.yml>```

## Output by type

**String dimensions** (cardinality ≤ 100 → full value list; >100 → top 20):
```json
{
  "member": "orders.status",
  "type": "string",
  "profile": {
    "cardinality": 4,
    "total_rows": 1000,
    "null_count": 0,
    "values": ["completed", "pending", "cancelled", "returned"],
    "top_values": [
      {"value": "completed", "count": 650},
      {"value": "pending", "count": 200},
      {"value": "cancelled", "count": 100},
      {"value": "returned", "count": 50}
    ]
  }
}
```

**Number dimensions**:
```json
{
  "member": "orders.amount",
  "type": "number",
  "profile": {
    "min": 0, "max": 999.99, "mean": 45.67,
    "distinct_count": 150, "null_count": 5, "total_rows": 1000
  }
}
```

**Date/datetime dimensions**:
```json
{
  "member": "orders.created_at",
  "type": "datetime",
  "profile": {
    "min": "2024-01-01T00:00:00Z", "max": "2025-06-15T23:59:59Z",
    "null_count": 0, "total_rows": 1000
  }
}
```

**Boolean dimensions**:
```json
{
  "member": "orders.is_active",
  "type": "boolean",
  "profile": {
    "true_count": 800, "false_count": 195,
    "null_count": 5, "total_rows": 1000
  }
}
```

## When to profile

- **After bootstrapping**: Verify the generated views reflect the actual data
- **Before writing filters**: Discover valid filter values instead of guessing
- **When debugging**: Check if a dimension has nulls, unexpected values, or wrong cardinality
- **To understand data freshness**: Date ranges show how current the data is

## Tips

- High-cardinality string dimensions (>100 values) only show the top 20 by frequency — this tells you it's not an enumerable field
- Profile output uses the `expr` from the view, so it reflects any transformations (e.g., `LOWER(status)`)
- Profiling requires a database connection (`--config`) and an `exec-*` feature flag
