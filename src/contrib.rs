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
#[derive(Debug, Deserialize)]
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

impl Default for RepoExpectations {
    fn default() -> Self {
        Self {
            views_min: default_views_min(),
            sample_queries: Vec::new(),
        }
    }
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
        assert_eq!(
            manifest.expect.sample_queries[0].measures,
            vec!["orders.count"]
        );
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
