//! Generic test runner for contributed foreign model repos.
//!
//! Iterates every subdirectory of `contrib/` that contains a `repo.yml` manifest,
//! converts the foreign models, and validates against the manifest expectations.
//!
//! Run: cargo test --test contrib_tests

use airlayer::contrib::load_manifest;
use airlayer::dialect::Dialect;
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

        assert_eq!(
            manifest.name, repo_name,
            "[{}] Manifest name '{}' does not match directory name",
            repo_name, manifest.name
        );

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
                    repo_name,
                    manifest.expect.views_min,
                    result.views.len()
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
                        panic!("[{}] sample_query[{}] failed: {}", repo_name, i, e);
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
