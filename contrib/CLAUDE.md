# contrib/ — Community Foreign Model Repos

This directory contains community-contributed foreign semantic model repos for parser stress testing.

## Adding a new repo

1. Create a directory named `<descriptive-name>-<format>` (e.g., `myproject-lookml`)
2. Copy the model files into it
3. Create a `repo.yml` manifest (see below)
4. Validate: `cargo test --test contrib_tests -- --nocapture`

## `repo.yml` manifest format

```yaml
name: my-project-lookml                    # must match directory name
description: "What this repo exercises"
format: lookml                             # cube | lookml | dbt | omni
contributor: "@github-handle"
upstream: "https://github.com/..."         # optional

expect:
  views_min: 3                             # minimum views that should parse
  sample_queries:                          # optional queries to compile
    - measures: [view_name.measure_name]
      dimensions: [view_name.dim_name]

known_issues:                              # optional: known parser gaps
  - "feature X not yet supported"
```

## Supported formats

| Format | Key | File extensions |
|--------|-----|-----------------|
| Cube.js | `cube` | `.yml`, `.yaml` (files with `cubes:` key) |
| LookML | `lookml` | `.lkml` (views, explores, models) |
| dbt MetricFlow | `dbt` | `.yml`, `.yaml` (files with `semantic_models:` key) |
| Omni | `omni` | `.yml`, `.yaml` (directory format or `views:` + `topics:` keys) |

## Testing

```bash
cargo test --test contrib_tests -- --nocapture
```

This runs the generic test runner which:
1. Scans all `contrib/*/repo.yml` manifests
2. Converts each repo using `foreign::convert_directory()`
3. Asserts `views.len() >= expect.views_min`
4. Compiles each `sample_queries` entry to SQL
5. Repos with `known_issues` warn on failure instead of failing

## `known_issues`

Use when the parser doesn't support a construct yet — the repo stays in CI as a tracked gap. Don't use for crashes or incorrect SQL (file a bug instead).
