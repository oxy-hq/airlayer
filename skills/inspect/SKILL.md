---
name: inspect
description: Inspect semantic layer views, dimensions, measures, and entities. Use when the user wants to explore what's available in their .view.yml files or understand the schema structure.
allowed-tools:
  - Bash
  - Read
  - Glob
argument-hint: "[--path <directory>] [--view <view_name>]"
---

# o3 Inspect — Explore Semantic Layer Schema

List all views, dimensions, measures, and entities defined in `.view.yml` files.

## Prerequisites

```bash
which o3 || cargo install --git https://github.com/oxy-hq/o3
```

## Inspect

```bash
# All views
o3 inspect --path <base_dir>

# Specific view
o3 inspect --path <base_dir> --view <view_name>
```

$ARGUMENTS

## Output format

For each view, shows:
- View name, description, table/sql source
- Entities (name, type, keys) — these drive auto-joins
- Dimensions (name, type, expression)
- Measures (name, aggregation type, expression)

Use this to understand what members are available before building queries with `/o3:query`.
