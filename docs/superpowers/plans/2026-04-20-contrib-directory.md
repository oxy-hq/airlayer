# Contrib Directory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a `contrib/` directory where community members can vendor foreign semantic model repos with a `repo.yml` manifest, validated by a single generic integration test.

**Architecture:** A `repo.yml` manifest per contributed repo declares format, expected view count, sample queries, and known issues. A single `tests/contrib_tests.rs` iterates all `contrib/*/repo.yml` entries, runs `foreign::convert_directory()`, and validates against the manifest. Repos with `known_issues` that fail are warnings, not failures.

**Tech Stack:** Rust (serde for manifest parsing), existing `airlayer::schema::foreign` module, cargo test.

---

### Task 1: Create manifest types and parser

**Files:**
- Create: `src/contrib.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test for manifest deserialization**

Create `src/contrib.rs` with the manifest types and a test:

```rust
//! Contrib repo manifest parsing.
//!
//! Each contributed repo in `contrib/` has a `repo.yml` manifest that declares
//! format, expected view count, sample queries, and known parser issues.

use serde::Deserialize;

use crate::engine::query::QueryRequest;
use crate::schema::foreign::ForeignFormat;

/// Manifest for a contributed foreign model repo (`repo.yml`).
#[derive(Debug, Deserialize)]
pub struct RepoManifest {
    /// Unique name (should match directory name).
    pub name: String,
    /// What this repo exercises.
    pub description: String,
    /// Foreign format: cube, lookml, dbt, omni.
    #[serde(deserialize_with = "deserialize_format")]
    pub format: ForeignFormat,
    /// Who contributed this repo.
    pub contributor: String,
    /// Optional URL of the original upstream repository.
    #[serde(default)]
    pub upstream: Option<String>,
    /// Validation expectations.
    #[serde(default)]
    pub expect: RepoExpectations,
    /// Known parser gaps — if present and conversion fails, test warns instead of failing.
    #[serde(default)]
    pub known_issues: Vec<String>,
}

/// What the test runner validates for this repo.
#[derive(Debug, Default, Deserialize)]
pub struct RepoExpectations {
    /// Minimum number of views that should parse.
    #[serde(default = "default_views_min")]
    pub views_min: usize,
    /// Optional queries that should compile to SQL.
    #[serde(default)]
    pub sample_queries: Vec<QueryRequest>,
}

fn default_views_min() -> usize {
    1
}

fn deserialize_format<'de, D>(deserializer: D) -> Result<ForeignFormat, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    ForeignFormat::parse_name(&s)
        .ok_or_else(|| serde::de::Error::custom(format!("unknown format: {}", s)))
}

/// Load a `repo.yml` manifest from a directory.
pub fn load_manifest(dir: &std::path::Path) -> Result<RepoManifest, String> {
    let manifest_path = dir.join("repo.yml");
    let content = std::fs::read_to_string(&manifest_path)
        .map_err(|e| format!("Failed to read {}: {}", manifest_path.display(), e))?;
    serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse {}: {}", manifest_path.display(), e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_manifest() {
        let yaml = r#"
name: test-repo
description: "A test repo"
format: lookml
contributor: "@test"
expect:
  views_min: 2
"#;
        let manifest: RepoManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.name, "test-repo");
        assert_eq!(manifest.format, ForeignFormat::LookML);
        assert_eq!(manifest.expect.views_min, 2);
        assert!(manifest.known_issues.is_empty());
    }

    #[test]
    fn parse_manifest_with_known_issues() {
        let yaml = r#"
name: test-repo
description: "A test repo"
format: cube
contributor: "@test"
known_issues:
  - "derived tables not supported"
  - "sql_trigger not parsed"
expect:
  views_min: 1
  sample_queries:
    - measures: ["orders.count"]
      dimensions: ["orders.status"]
"#;
        let manifest: RepoManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.format, ForeignFormat::Cube);
        assert_eq!(manifest.known_issues.len(), 2);
        assert_eq!(manifest.expect.sample_queries.len(), 1);
        assert_eq!(manifest.expect.sample_queries[0].measures, vec!["orders.count"]);
    }

    #[test]
    fn parse_manifest_defaults() {
        let yaml = r#"
name: bare
description: "Bare minimum"
format: dbt
contributor: "@min"
"#;
        let manifest: RepoManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.expect.views_min, 1);
        assert!(manifest.expect.sample_queries.is_empty());
        assert!(manifest.known_issues.is_empty());
        assert!(manifest.upstream.is_none());
    }
}
```

- [ ] **Step 2: Add `pub mod contrib;` to `src/lib.rs`**

Add after the existing module declarations in `src/lib.rs`:

```rust
pub mod contrib;
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test contrib --lib`
Expected: 3 tests pass (parse_minimal_manifest, parse_manifest_with_known_issues, parse_manifest_defaults)

- [ ] **Step 4: Commit**

```bash
git add src/contrib.rs src/lib.rs
git commit -m "feat: add contrib repo manifest types and parser"
```

---

### Task 2: Migrate LookML repos to `contrib/` with manifests

**Files:**
- Create: `contrib/jira-lookml/repo.yml` (+ move files from `tests/integration/lookml_repos/jira/`)
- Create: `contrib/google-ads-lookml/repo.yml` (+ move files from `tests/integration/lookml_repos/google_ads/`)
- Create: `contrib/healthcare-lookml/repo.yml` (+ move files from `tests/integration/lookml_repos/healthcare/`)
- Create: `contrib/sales-lookml/repo.yml` (+ move files from `tests/integration/lookml_repos/sales/`)

- [ ] **Step 1: Move files and create manifests**

```bash
# Move repos
mv tests/integration/lookml_repos/jira contrib/jira-lookml
mv tests/integration/lookml_repos/google_ads contrib/google-ads-lookml
mv tests/integration/lookml_repos/healthcare contrib/healthcare-lookml
mv tests/integration/lookml_repos/sales contrib/sales-lookml
rmdir tests/integration/lookml_repos
```

Create `contrib/jira-lookml/repo.yml`:

```yaml
name: jira-lookml
description: "Jira LookML block — exercises @{CONSTANT} in sql_table_name, dimension_group timeframes, yesno dimensions, measure filters, explore joins with 4 relationships"
format: lookml
contributor: "@airlayer"
upstream: "https://github.com/looker-open-source/block-jira-new"
expect:
  views_min: 4
  sample_queries:
    - measures: ["issue.count"]
      dimensions: ["issue.status"]
    - measures: ["issue.count"]
      dimensions: ["priority.name"]
```

Create `contrib/google-ads-lookml/repo.yml`:

```yaml
name: google-ads-lookml
description: "Google Ads LookML block — exercises running_total measures, count_distinct, date_raw dimensions, 10-timeframe dimension_groups, cross-field refs, explore joins"
format: lookml
contributor: "@airlayer"
upstream: "https://github.com/looker-open-source/block-google-ads"
expect:
  views_min: 3
  sample_queries:
    - measures: ["ad_impressions.total_impressions"]
      dimensions: ["ad_impressions.ad_network_type1"]
    - measures: ["ad_impressions.total_clicks"]
      dimensions: ["campaign.name"]
```

Create `contrib/healthcare-lookml/repo.yml`:

```yaml
name: healthcare-lookml
description: "Healthcare LookML demo — exercises duration dimension_groups, yesno/zipcode types, filtered measures, multiple measure types (sum/avg/count_distinct), explore joins"
format: lookml
contributor: "@airlayer"
upstream: "https://github.com/looker-open-source/healthcare_demo"
expect:
  views_min: 3
  sample_queries:
    - measures: ["encounter.count"]
      dimensions: ["encounter.encounter_class"]
    - measures: ["encounter.count"]
      dimensions: ["patient.gender"]
```

Create `contrib/sales-lookml/repo.yml`:

```yaml
name: sales-lookml
description: "Sales LookML block — exercises 6-timeframe dimension_groups, yesno dimensions, filtered measures, explore joins"
format: lookml
contributor: "@airlayer"
upstream: "https://github.com/looker-open-source/block-sales"
expect:
  views_min: 2
  sample_queries:
    - measures: ["opportunity.total_amount", "opportunity.count"]
      dimensions: ["opportunity.stage_name"]
    - measures: ["opportunity.total_amount"]
      dimensions: ["account.industry"]
```

- [ ] **Step 2: Update `lookml_parity_tests.rs` paths**

Change the helper function path from `tests/integration/lookml_repos` to `contrib`:

```rust
fn load_lookml_engine(fixture_dir: &str) -> SemanticEngine {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contrib")
        .join(fixture_dir);
```

And update all call sites to use the new directory names:
- `"jira"` → `"jira-lookml"`
- `"google_ads"` → `"google-ads-lookml"`
- `"healthcare"` → `"healthcare-lookml"`
- `"sales"` → `"sales-lookml"`

Also update the bulk compile test:
```rust
let fixtures = vec!["jira-lookml", "google-ads-lookml", "healthcare-lookml", "sales-lookml"];
```

- [ ] **Step 3: Run existing LookML parity tests to verify nothing broke**

Run: `cargo test --test lookml_parity_tests`
Expected: All 28 tests pass.

- [ ] **Step 4: Commit**

```bash
git add contrib/ tests/lookml_parity_tests.rs
git rm -r tests/integration/lookml_repos
git commit -m "refactor: migrate LookML test repos to contrib/ with manifests"
```

---

### Task 3: Write `contrib_tests.rs` — the generic test runner

**Files:**
- Create: `tests/contrib_tests.rs`

- [ ] **Step 1: Write the generic test**

Create `tests/contrib_tests.rs`:

```rust
//! Generic test runner for contributed foreign model repos.
//!
//! Iterates every subdirectory of `contrib/` that contains a `repo.yml` manifest,
//! converts the foreign models, and validates against the manifest expectations.
//!
//! Run: cargo test --test contrib_tests

use airlayer::contrib::load_manifest;
use airlayer::dialect::Dialect;
use airlayer::engine::query::QueryRequest;
use airlayer::engine::{DatasourceDialectMap, SemanticEngine};
use airlayer::schema::foreign;
use airlayer::schema::models::SemanticLayer;
use std::path::Path;

fn discover_contrib_repos() -> Vec<std::path::PathBuf> {
    let contrib_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("contrib");
    if !contrib_dir.exists() {
        return vec![];
    }
    let mut repos: Vec<_> = std::fs::read_dir(&contrib_dir)
        .expect("Failed to read contrib/")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() && path.join("repo.yml").exists() {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    repos.sort();
    repos
}

#[test]
fn contrib_repos_parse_and_validate() {
    let repos = discover_contrib_repos();
    assert!(
        !repos.is_empty(),
        "No contrib repos found — expected at least one contrib/*/repo.yml"
    );

    let mut total_repos = 0;
    let mut total_views = 0;
    let mut total_queries = 0;
    let mut warned_repos = Vec::new();

    for repo_dir in &repos {
        let repo_name = repo_dir.file_name().unwrap().to_str().unwrap();
        let manifest = load_manifest(repo_dir)
            .unwrap_or_else(|e| panic!("[{}] Failed to load manifest: {}", repo_name, e));

        let has_known_issues = !manifest.known_issues.is_empty();

        // Step 1: Convert foreign models
        let conversion = foreign::convert_directory(manifest.format, repo_dir);
        let result = match conversion {
            Ok(r) => r,
            Err(e) => {
                if has_known_issues {
                    eprintln!(
                        "[{}] WARN: conversion failed (known issues: {:?}): {}",
                        repo_name, manifest.known_issues, e
                    );
                    warned_repos.push(repo_name.to_string());
                    total_repos += 1;
                    continue;
                } else {
                    panic!("[{}] Conversion failed: {}", repo_name, e);
                }
            }
        };

        // Step 2: Check minimum view count
        if result.views.len() < manifest.expect.views_min {
            if has_known_issues {
                eprintln!(
                    "[{}] WARN: expected >= {} views, got {} (known issues)",
                    repo_name, manifest.expect.views_min, result.views.len()
                );
                warned_repos.push(repo_name.to_string());
                total_repos += 1;
                continue;
            } else {
                panic!(
                    "[{}] Expected >= {} views, got {}",
                    repo_name,
                    manifest.expect.views_min,
                    result.views.len()
                );
            }
        }

        total_views += result.views.len();

        // Step 3: Build engine and compile sample queries
        let layer = SemanticLayer::new(result.views, None);
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = match SemanticEngine::from_semantic_layer(layer, dialects) {
            Ok(e) => e,
            Err(e) => {
                if has_known_issues {
                    eprintln!(
                        "[{}] WARN: engine build failed (known issues): {}",
                        repo_name, e
                    );
                    warned_repos.push(repo_name.to_string());
                    total_repos += 1;
                    continue;
                } else {
                    panic!("[{}] Engine build failed: {}", repo_name, e);
                }
            }
        };

        for (i, query) in manifest.expect.sample_queries.iter().enumerate() {
            match engine.compile_query(query) {
                Ok(compiled) => {
                    assert!(
                        compiled.sql.contains("SELECT"),
                        "[{}] sample_query[{}]: compiled SQL missing SELECT:\n{}",
                        repo_name,
                        i,
                        compiled.sql
                    );
                    total_queries += 1;
                }
                Err(e) => {
                    if has_known_issues {
                        eprintln!(
                            "[{}] WARN: sample_query[{}] failed (known issues): {}",
                            repo_name, i, e
                        );
                    } else {
                        panic!(
                            "[{}] sample_query[{}] failed: {}",
                            repo_name, i, e
                        );
                    }
                }
            }
        }

        total_repos += 1;
    }

    eprintln!(
        "\n=== Contrib summary: {} repos, {} views, {} queries compiled ===",
        total_repos, total_views, total_queries
    );
    if !warned_repos.is_empty() {
        eprintln!("  Repos with warnings: {}", warned_repos.join(", "));
    }
}
```

- [ ] **Step 2: Run the contrib test**

Run: `cargo test --test contrib_tests`
Expected: 1 test passes, summary shows 4 repos, views, and queries compiled.

- [ ] **Step 3: Commit**

```bash
git add tests/contrib_tests.rs
git commit -m "feat: add generic contrib test runner for foreign model repos"
```

---

### Task 4: Write `contrib/README.md`

**Files:**
- Create: `contrib/README.md`

- [ ] **Step 1: Write the README**

Create `contrib/README.md`:

```markdown
# Contributing Foreign Model Repos

This directory contains community-contributed semantic model repositories used to stress-test airlayer's foreign model parsers (Cube.js, LookML, dbt MetricFlow, Omni).

Each subdirectory is a self-contained foreign model repo with a `repo.yml` manifest. A generic test runner validates all repos automatically.

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
```

- [ ] **Step 2: Commit**

```bash
git add contrib/README.md
git commit -m "docs: add contrib README with contribution guide and manifest reference"
```

---

### Task 5: Write `contrib/CLAUDE.md`

**Files:**
- Create: `contrib/CLAUDE.md`

- [ ] **Step 1: Write the CLAUDE.md**

Create `contrib/CLAUDE.md`:

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add contrib/CLAUDE.md
git commit -m "docs: add contrib/CLAUDE.md for Claude Code contributors"
```

---

### Task 6: Add `just` recipe and update root `CLAUDE.md`

**Files:**
- Modify: `Justfile`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add `test-contrib` recipe to `Justfile`**

Add after the existing test recipes:

```just
# Run contrib foreign model repo tests
test-contrib:
    cargo test --test contrib_tests -- --nocapture
```

- [ ] **Step 2: Update `CLAUDE.md` project structure section**

Add `contrib/` to the project structure tree, after the `tests/` section:

```
contrib/                    Community-contributed foreign model repos
├── CLAUDE.md               Instructions for contributors using Claude Code
├── README.md               Contribution guide and manifest reference
├── <name>-<format>/        Each contributed repo
│   ├── repo.yml            Manifest (format, expectations, known issues)
│   └── *.lkml / *.yml      Model files
```

Update the test counts table to include contrib tests.

Add to the "Build & test" section:

```bash
just test-contrib         # contrib foreign model repo tests
```

- [ ] **Step 3: Update `CLAUDE.md` foreign model support section**

Add a note about `contrib/`:

```
Community-contributed repos live in `contrib/` — see `contrib/README.md` for how to add new repos.
```

- [ ] **Step 4: Run all tests to verify nothing is broken**

Run: `cargo test --test contrib_tests -- --nocapture && cargo test --test lookml_parity_tests`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add Justfile CLAUDE.md
git commit -m "feat: add test-contrib recipe and update docs for contrib directory"
```
