---
name: inspect
description: Inspect semantic layer views, dimensions, measures, and entities. Use when the user wants to explore what's available in their .view.yml files or understand the schema structure.
allowed-tools:
  - Bash
  - Read
  - Glob
argument-hint: "[--path <directory>] [--view <view_name>]"
---

# airlayer Inspect — Explore Semantic Layer Schema

List all views, dimensions, measures, and entities defined in `.view.yml` files.

## Prerequisites

```bash
which airlayer || cargo install --git https://github.com/oxy-hq/airlayer
```

## Inspect

```bash
# All views
airlayer inspect --path <base_dir>

# Specific view
airlayer inspect --path <base_dir> --view <view_name>
```

$ARGUMENTS

## Output format

For each view, shows:
- View name, description, table/sql source
- Entities (name, type, keys) — these drive auto-joins
- Dimensions (name, type, expression)
- Measures (name, aggregation type, expression)

Use this to understand what members are available before building queries with `/airlayer:query`.
