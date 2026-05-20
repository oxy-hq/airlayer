# Contributing Foreign Model Repos

This directory contains community-contributed semantic model repositories used to stress-test airlayer's foreign model parsers (Cube.js, LookML, dbt MetricFlow, Omni).

Each subdirectory is a self-contained foreign model repo with a `repo.yml` manifest. A generic test runner validates all repos automatically.

## Getting started

The easiest way to contribute is to use [Claude Code](https://docs.anthropic.com/en/docs/claude-code) inside your new repo directory. The `contrib/CLAUDE.md` file gives Claude all the context it needs about the manifest format and testing workflow.

```bash
cd contrib/
mkdir myproject-lookml
cp /path/to/your/lookml/files/* myproject-lookml/
cd myproject-lookml
claude
```

From there, Claude can help you write the `repo.yml` manifest, run the tests, and iterate on any parser issues.

## How to contribute

1. **Create a directory** named `<descriptive-name>-<format>` (e.g., `myproject-lookml`, `ecommerce-cube`)
2. **Copy your model files** into the directory
3. **Write a `repo.yml` manifest** (see format below)
4. **Run the tests**: `cargo test --test contrib_tests`
5. **Open a PR**

## Manifest format (`repo.yml`)

```yaml
name: my-project-lookml                    # must match directory name
description: "What this repo exercises"    # be specific about edge cases
format: lookml                             # cube | lookml | dbt | omni
contributor: "@your-github-handle"
upstream: "https://github.com/..."         # optional: original repo URL

expect:
  views_min: 3                             # minimum views that should parse
  sample_queries:                          # optional: queries to compile
    - measures: [view_name.measure_name]
      dimensions: [view_name.dimension_name]

# If the parser doesn't fully support your repo yet, list known gaps here.
# Repos with known_issues won't break CI when they fail.
known_issues:
  - "derived_table with sql_trigger not yet supported"
```

### Required fields

| Field | Description |
|-------|-------------|
| `name` | Unique identifier, must match directory name |
| `description` | What parser edge cases this repo exercises |
| `format` | One of: `cube`, `lookml`, `dbt`, `omni` |
| `contributor` | Your GitHub handle |
| `expect.views_min` | Minimum number of views that should parse (default: 1) |

### Optional fields

| Field | Description |
|-------|-------------|
| `upstream` | URL of the original repository for provenance |
| `expect.sample_queries` | Queries to compile (default dialect: Postgres) |
| `known_issues` | List of known parser gaps — prevents CI failure |

## When to use `known_issues`

Use `known_issues` when:
- The parser doesn't support a construct your repo uses (e.g., a LookML feature airlayer hasn't implemented yet)
- You want to contribute the repo now so the gap is tracked, even before it's fixed

Don't use `known_issues` when:
- The parser crashes or panics — file a bug instead
- The parser produces incorrect SQL — file a bug instead

As parser support improves, maintainers will remove entries from `known_issues` and the tests will automatically tighten.

## What makes a good contribution

The most valuable repos exercise **real-world edge cases** that synthetic test fixtures miss:

- Unusual LookML features (liquid templates, constants, derived tables)
- Complex Cube.js schemas (polymorphic joins, pre-aggregations, context variables)
- dbt MetricFlow patterns (derived metrics, cumulative metrics, SCD models)
- Omni patterns (custom SQL, relationships, dimension groups)
- Multi-file projects with cross-file references
- Large schemas that stress the parser

## Running tests

```bash
# Run just the contrib tests
cargo test --test contrib_tests

# Run with output to see the summary
cargo test --test contrib_tests -- --nocapture
```
