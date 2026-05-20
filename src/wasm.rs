//! WebAssembly bindings for airlayer.
//!
//! Provides a JS-friendly API for compiling semantic queries to SQL,
//! and pre-aggregation cache management via IndexedDB.
//!
//! Build with (includes foreign model support):
//!   wasm-pack build --target web --no-default-features --features wasm,foreign
//!
//! For a minimal build without foreign parsers:
//!   wasm-pack build --target web --no-default-features --features wasm

use wasm_bindgen::prelude::*;

use crate::dialect::Dialect;
use crate::engine::catalog;
use crate::engine::preagg;
use crate::engine::query::QueryRequest;
use crate::engine::{DatasourceDialectMap, SemanticEngine};
use crate::schema::models::SemanticLayer;
use crate::schema::parser::SchemaParser;

/// Helper: parse a Vec<JsValue> of YAML strings into a typed Vec using a parser function.
fn parse_yaml_array<T>(
    items: &[JsValue],
    label: &str,
    parse_fn: impl Fn(&str, &str) -> Result<T, String>,
) -> Result<Vec<T>, JsValue> {
    let mut result = Vec::new();
    for (i, val) in items.iter().enumerate() {
        let yaml_str = val
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("{label}[{i}] is not a string")))?;
        let item =
            parse_fn(&yaml_str, &format!("<{label}_{i}>")).map_err(|e| JsValue::from_str(&e))?;
        result.push(item);
    }
    Ok(result)
}

/// Compile a semantic query to SQL.
///
/// # Arguments
/// - `views_yaml`: Array of .view.yml file contents (YAML strings)
/// - `query_json`: Query as JSON (same format as `airlayer query -q`)
/// - `dialect`: SQL dialect string (e.g., "postgres", "bigquery", "duckdb")
/// - `topics_yaml`: Optional array of .topic.yml file contents
/// - `motifs_yaml`: Optional array of .motif.yml file contents
/// - `queries_yaml`: Optional array of .query.yml file contents (saved queries)
///
/// # Returns
/// JSON object with `sql`, `params`, and `columns` fields.
#[wasm_bindgen]
pub fn compile(
    views_yaml: Vec<JsValue>,
    query_json: &str,
    dialect: &str,
    topics_yaml: Option<Vec<JsValue>>,
    motifs_yaml: Option<Vec<JsValue>>,
    queries_yaml: Option<Vec<JsValue>>,
) -> Result<JsValue, JsValue> {
    let parser = SchemaParser::new();

    let views = parse_yaml_array(&views_yaml, "views", |y, s| parser.parse_view_str(y, s))?;

    let topics = match topics_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "topics", |y, s| {
            parser.parse_topic_str(y, s)
        })?),
        _ => None,
    };

    let motifs = match motifs_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "motifs", |y, s| {
            parser.parse_motif_str(y, s)
        })?),
        _ => None,
    };

    let saved_queries = match queries_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "queries", |y, s| {
            parser.parse_saved_query_str(y, s)
        })?),
        _ => None,
    };

    let layer = SemanticLayer::with_motifs_and_queries(views, topics, motifs, saved_queries);

    let resolved_dialect = Dialect::from_str(dialect)
        .ok_or_else(|| JsValue::from_str(&format!("Unknown dialect: {}", dialect)))?;

    let mut dialect_map = DatasourceDialectMap::new();
    dialect_map.set_default(resolved_dialect);

    let engine = SemanticEngine::from_semantic_layer(layer, dialect_map)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let request: QueryRequest = serde_json::from_str(query_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid query JSON: {}", e)))?;

    let result = engine
        .compile_query(&request)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    serde_wasm_bindgen::to_value(&result).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Validate view YAML without compiling a query.
///
/// # Arguments
/// - `views_yaml`: Array of .view.yml file contents (YAML strings)
/// - `topics_yaml`: Optional array of .topic.yml file contents
///
/// # Returns
/// `true` if all views parse and validate successfully.
/// Throws on error with a descriptive message.
#[wasm_bindgen]
pub fn validate(
    views_yaml: Vec<JsValue>,
    topics_yaml: Option<Vec<JsValue>>,
) -> Result<bool, JsValue> {
    let parser = SchemaParser::new();

    let views = parse_yaml_array(&views_yaml, "views", |y, s| parser.parse_view_str(y, s))?;

    let topics = match topics_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "topics", |y, s| {
            parser.parse_topic_str(y, s)
        })?),
        _ => None,
    };

    let layer = SemanticLayer::new(views, topics);

    crate::schema::validator::SchemaValidator::validate(&layer)
        .map_err(|e| JsValue::from_str(&e))?;

    Ok(true)
}

/// List all semantic objects (views, dimensions, measures, motifs, etc.).
///
/// # Arguments
/// - `views_yaml`: Array of .view.yml file contents (YAML strings)
/// - `topics_yaml`: Optional array of .topic.yml file contents
/// - `motifs_yaml`: Optional array of .motif.yml file contents
/// - `queries_yaml`: Optional array of .query.yml file contents (saved queries)
///
/// # Returns
/// JSON array of catalog entries.
#[wasm_bindgen]
pub fn catalog_list(
    views_yaml: Vec<JsValue>,
    topics_yaml: Option<Vec<JsValue>>,
    motifs_yaml: Option<Vec<JsValue>>,
    queries_yaml: Option<Vec<JsValue>>,
) -> Result<JsValue, JsValue> {
    let parser = SchemaParser::new();

    let views = parse_yaml_array(&views_yaml, "views", |y, s| parser.parse_view_str(y, s))?;

    let topics = match topics_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "topics", |y, s| {
            parser.parse_topic_str(y, s)
        })?),
        _ => None,
    };

    let motifs = match motifs_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "motifs", |y, s| {
            parser.parse_motif_str(y, s)
        })?),
        _ => None,
    };

    let saved_queries = match queries_yaml {
        Some(ref arr) if !arr.is_empty() => Some(parse_yaml_array(arr, "queries", |y, s| {
            parser.parse_saved_query_str(y, s)
        })?),
        _ => None,
    };

    let layer = SemanticLayer::with_motifs_and_queries(views, topics, motifs, saved_queries);
    let entries = catalog::catalog(&layer);

    serde_wasm_bindgen::to_value(&entries).map_err(|e| JsValue::from_str(&e.to_string()))
}

// ---------------------------------------------------------------------------
// Pre-aggregation cache API
//
// These functions enable browser-based pre-aggregation caching. The WASM
// module handles pure computation (coverage checking, SQL generation). The
// JavaScript caller handles I/O (IndexedDB storage, duckdb-wasm execution).
//
// Typical flow:
//   1. JS fetches rollup data from warehouse and calls `cache_build_manifest`
//      to persist the manifest. Rollup data (JSON rows) is stored in
//      IndexedDB by the JS caller using the cache key.
//   2. On query, JS reads the manifest and calls `cache_resolve` to check
//      if a cached rollup covers the query.
//   3. If resolved, JS loads the cached data into a duckdb-wasm table
//      named `__cache` and executes the returned `reagg_sql`.
// ---------------------------------------------------------------------------

/// Check if a cached rollup covers a query and return re-aggregation SQL.
///
/// # Arguments
/// - `manifest_json`: The local manifest JSON (from `cache_build_manifest` or IndexedDB)
/// - `query_json`: The query as JSON (same format as `airlayer query -q`)
///
/// # Returns
/// JSON object with `reagg_sql`, `cache_key`, and `entry` fields if a rollup
/// covers the query. Returns `null` if no rollup matches.
///
/// The `reagg_sql` reads from a table named `"__cache"` — the JS caller must
/// create this table in duckdb-wasm with the cached rollup data before executing.
#[wasm_bindgen]
pub fn cache_resolve(manifest_json: &str, query_json: &str) -> Result<JsValue, JsValue> {
    let manifest: preagg::LocalManifest = serde_json::from_str(manifest_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid manifest JSON: {}", e)))?;

    let request: QueryRequest = serde_json::from_str(query_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid query JSON: {}", e)))?;

    match preagg::resolve_cached(&request, &manifest) {
        Some(resolution) => {
            serde_wasm_bindgen::to_value(&resolution).map_err(|e| JsValue::from_str(&e.to_string()))
        }
        None => Ok(JsValue::NULL),
    }
}

/// Parse warehouse manifest rows into a local manifest JSON string.
///
/// # Arguments
/// - `rows_json`: JSON array of manifest rows from the warehouse `__manifest` table.
///   Each row is an object with fields: `view_name`, `rollup_name`, `rollup_hash`,
///   `table_name`, `dimensions`, `measures`, `time_dimension`, `granularity`, `build_date`.
/// - `source_database`: Name of the source database (for metadata).
///
/// # Returns
/// A JSON string representing the `LocalManifest`, ready to be stored in IndexedDB
/// and passed to `cache_resolve`.
#[wasm_bindgen]
pub fn cache_build_manifest(rows_json: &str, source_database: &str) -> Result<String, JsValue> {
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(rows_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid rows JSON: {}", e)))?;

    let warehouse_entries = preagg::parse_manifest_rows(&rows);

    let local_entries: Vec<preagg::LocalRollupEntry> = warehouse_entries
        .iter()
        .map(|e| {
            let mut local = e.to_local_entry();
            local.file = format!("{}__{}", e.view_name, e.rollup_hash);
            local
        })
        .collect();

    let manifest = preagg::LocalManifest {
        pulled_at: String::new(), // JS caller can set this
        source_database: source_database.to_string(),
        rollups: local_entries,
    };

    serde_json::to_string(&manifest)
        .map_err(|e| JsValue::from_str(&format!("Failed to serialize manifest: {}", e)))
}

/// Get the cache key for a warehouse rollup entry.
///
/// # Arguments
/// - `view_name`: The view name
/// - `rollup_hash`: The rollup hash
///
/// # Returns
/// The cache key string (e.g., `"events__a1b2c3d4"`), used as the IndexedDB key.
#[wasm_bindgen]
pub fn cache_key(view_name: &str, rollup_hash: &str) -> String {
    format!("{}__{}", view_name, rollup_hash)
}

/// Resolve a query against warehouse rollup entries (for Layer 2 cache).
///
/// # Arguments
/// - `rows_json`: JSON array of manifest rows from the warehouse
/// - `query_json`: The query as JSON
/// - `schema`: The pre-aggregation schema name
/// - `dialect`: SQL dialect string
///
/// # Returns
/// JSON object with `reagg_sql` and `table_name` if a warehouse rollup covers
/// the query. Returns `null` if no rollup matches.
#[wasm_bindgen]
pub fn cache_resolve_warehouse(
    rows_json: &str,
    query_json: &str,
    schema: &str,
    dialect: &str,
) -> Result<JsValue, JsValue> {
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(rows_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid rows JSON: {}", e)))?;

    let request: QueryRequest = serde_json::from_str(query_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid query JSON: {}", e)))?;

    let resolved_dialect = Dialect::from_str(dialect)
        .ok_or_else(|| JsValue::from_str(&format!("Unknown dialect: {}", dialect)))?;

    let entries = preagg::parse_manifest_rows(&rows);

    match preagg::resolve_warehouse(&request, &entries, schema, &resolved_dialect) {
        Some(preagg::PreaggResolution::WarehouseRollup {
            reagg_sql,
            table_name,
        }) => {
            let result = serde_json::json!({
                "reagg_sql": reagg_sql,
                "table_name": table_name,
            });
            serde_wasm_bindgen::to_value(&result).map_err(|e| JsValue::from_str(&e.to_string()))
        }
        _ => Ok(JsValue::NULL),
    }
}

// ---------------------------------------------------------------------------
// Foreign model compilation
//
// Compile queries directly from foreign semantic model formats (Cube.js,
// LookML, dbt, Omni) without converting to .view.yml first.
//
// Each format is gated behind its own feature flag (foreign-cube,
// foreign-lookml, foreign-dbt, foreign-omni) so only the parsers you need
// are included in the WASM binary.
// ---------------------------------------------------------------------------

/// Compile a semantic query from foreign model files (Cube.js, LookML, dbt, Omni).
///
/// # Arguments
/// - `format`: Format string — "cube", "lookml", "dbt", or "omni"
/// - `files`: Array of file content strings (raw LookML, YAML, etc.)
/// - `query_json`: Query as JSON (same format as `airlayer query -q`)
/// - `dialect`: SQL dialect string (e.g., "postgres", "bigquery")
///
/// # Returns
/// JSON object with `sql`, `params`, and `columns` fields.
///
/// # Feature flags
/// Requires the corresponding `foreign-*` feature to be enabled at build time.
/// Build with e.g. `--features wasm,foreign-lookml` to include LookML support.
#[cfg(any(
    feature = "foreign-cube",
    feature = "foreign-lookml",
    feature = "foreign-dbt",
    feature = "foreign-omni"
))]
#[wasm_bindgen]
pub fn compile_foreign(
    format: &str,
    files: Vec<JsValue>,
    query_json: &str,
    dialect: &str,
) -> Result<JsValue, JsValue> {
    use crate::schema::foreign::{self, ForeignFormat};

    let fmt = ForeignFormat::parse_name(format)
        .ok_or_else(|| JsValue::from_str(&format!("Unknown format: {}", format)))?;

    let mut all_views = Vec::new();
    let mut all_warnings = Vec::new();

    for (i, val) in files.iter().enumerate() {
        let content = val
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("files[{}] is not a string", i)))?;
        match foreign::convert(fmt, &content, &format!("<file_{}>", i)) {
            Ok(result) => {
                all_views.extend(result.views);
                all_warnings.extend(result.warnings);
            }
            Err(e) => {
                all_warnings.push(format!("Skipping file {}: {}", i, e));
            }
        }
    }

    if all_views.is_empty() {
        return Err(JsValue::from_str(&format!(
            "No views converted from {} files. Warnings: {}",
            format,
            all_warnings.join("; ")
        )));
    }

    let layer = SemanticLayer::new(all_views, None);

    let resolved_dialect = Dialect::from_str(dialect)
        .ok_or_else(|| JsValue::from_str(&format!("Unknown dialect: {}", dialect)))?;

    let mut dialect_map = DatasourceDialectMap::new();
    dialect_map.set_default(resolved_dialect);

    let engine = SemanticEngine::from_semantic_layer(layer, dialect_map)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let request: QueryRequest = serde_json::from_str(query_json)
        .map_err(|e| JsValue::from_str(&format!("Invalid query JSON: {}", e)))?;

    let result = engine
        .compile_query(&request)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    serde_wasm_bindgen::to_value(&result).map_err(|e| JsValue::from_str(&e.to_string()))
}
