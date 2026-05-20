# Design: `contrib/` directory for community foreign model repos

**Date:** 2026-04-20
**Status:** Approved

## Problem

airlayer supports four foreign semantic model formats (Cube.js, LookML, dbt MetricFlow, Omni), but parser coverage is only tested against a small set of curated fixtures. Real-world repos exercise edge cases that synthetic fixtures miss. We need a low-friction way for users to contribute their own repos as parser stress tests.

## Solution

A top-level `contrib/` directory where community members vendor their foreign semantic model files alongside a `repo.yml` manifest. A single generic integration test iterates all contributed repos and validates parsing + optional query compilation.

## Directory structure

```
contrib/
├── CLAUDE.md                  # Instructions for contributors using Claude Code
├── README.md                  # How to contribute, manifest format docs
├── google-ads-lookml/
│   ├── repo.yml               # Manifest
│   ├── ad_impressions.view.lkml
│   └── ...
├── jaffle-shop-dbt/
│   ├── repo.yml
│   ├── models/semantic_models.yml
│   └── ...
└── acme-cube/
    ├── repo.yml
    ├── schema/Orders.yml
    └── ...
```

## Manifest format (`repo.yml`)

```yaml
name: google-ads-lookml
description: "Google Ads LookML block — exercises constants, filtered measures, explore joins"
format: lookml              # cube | lookml | dbt | omni
contributor: "@github_handle"
upstream: "https://github.com/looker/block-google-ads"  # optional, provenance URL

# What we validate
expect:
  views_min: 3              # at least N views must parse successfully
  sample_queries:           # optional: queries that should compile to SQL
    - measures: [ad_impressions.total_impressions]
      dimensions: [ad_impressions.campaign_name]

# Known parser gaps — repo is included in CI but won't fail on these
known_issues:
  - "derived_table with sql_trigger not yet supported"
```

### Required fields

- `name` — unique identifier, matches directory name
- `description` — what this repo exercises
- `format` — one of `cube`, `lookml`, `dbt`, `omni`
- `contributor` — GitHub handle or name
- `expect.views_min` — minimum number of views that should parse

### Optional fields

- `upstream` — URL of the original repository for provenance tracking
- `expect.sample_queries` — list of queries to compile (default dialect: postgres)
- `known_issues` — list of strings describing known parser gaps

## Test behavior

A single test file `tests/contrib_tests.rs`:

1. Scan every subdirectory of `contrib/` for a `repo.yml` file
2. Deserialize the manifest
3. Call `foreign::convert_directory()` with the declared format
4. Assert `views.len() >= expect.views_min`
5. For each entry in `expect.sample_queries`, build a `QueryRequest` and compile to SQL (default dialect: postgres)
6. **If `known_issues` is non-empty and conversion/compilation fails:** log a warning, test passes
7. **If `known_issues` is empty and anything fails:** test fails, CI breaks

This means:
- Repos without `known_issues` are expected to fully work — keeps CI honest
- Repos with `known_issues` track parser gaps without blocking merges
- As parser support improves, contributors (or maintainers) remove entries from `known_issues` and the assertions tighten automatically

## Migration

Move existing `tests/integration/lookml_repos/` fixtures (google_ads, healthcare, jira, sales) into `contrib/`, adding a `repo.yml` manifest to each. Update or retire `lookml_parity_tests.rs` in favor of the generic `contrib_tests.rs`.

## `contrib/CLAUDE.md`

A CLAUDE.md file in `contrib/` gives contributors who start Claude Code from that directory the context they need. It supplements the root CLAUDE.md (Claude Code merges up the tree) with:

- The manifest format and all fields
- Available formats: `cube`, `lookml`, `dbt`, `omni`
- How to validate: `cargo test --test contrib_tests`
- When to use `known_issues` (parser doesn't support a construct yet) vs filing a bug (parser crashes or produces wrong output)
- Naming convention: `<descriptive-name>-<format>` (e.g., `google-ads-lookml`, `jaffle-shop-dbt`)

## `contrib/README.md`

Human-readable documentation covering:

- Purpose of the directory
- Step-by-step contribution guide (copy files, write `repo.yml`, run tests, open PR)
- Full manifest reference with examples
- How `known_issues` works
- What makes a good contribution (real-world repos that exercise edge cases)

## Justification

- **`contrib/` at repo root** rather than nested in `tests/` or `examples/` — signals "community-contributed, open for submissions" and is discoverable
- **Vendored files** rather than git submodules — no external dependencies, works offline, no CI fragility
- **`upstream` field** for optional provenance tracking without submodule machinery
- **Single generic test** rather than per-repo hand-written tests — low friction to contribute, scales automatically
- **`known_issues` mechanism** — lets us accept repos that exercise unsupported constructs without breaking CI, while tracking the gaps
