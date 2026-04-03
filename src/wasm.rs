//! WebAssembly bindings for airlayer.
//!
//! Provides a JS-friendly API for compiling semantic queries to SQL.
//! Build with: `wasm-pack build --target web --no-default-features --features wasm`

use wasm_bindgen::prelude::*;

use crate::dialect::Dialect;
use crate::engine::catalog;
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
        let item = parse_fn(&yaml_str, &format!("<{label}_{i}>"))
            .map_err(|e| JsValue::from_str(&e))?;
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
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "topics", |y, s| parser.parse_topic_str(y, s))?)
        }
        _ => None,
    };

    let motifs = match motifs_yaml {
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "motifs", |y, s| parser.parse_motif_str(y, s))?)
        }
        _ => None,
    };

    let saved_queries = match queries_yaml {
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "queries", |y, s| parser.parse_saved_query_str(y, s))?)
        }
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
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "topics", |y, s| parser.parse_topic_str(y, s))?)
        }
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
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "topics", |y, s| parser.parse_topic_str(y, s))?)
        }
        _ => None,
    };

    let motifs = match motifs_yaml {
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "motifs", |y, s| parser.parse_motif_str(y, s))?)
        }
        _ => None,
    };

    let saved_queries = match queries_yaml {
        Some(ref arr) if !arr.is_empty() => {
            Some(parse_yaml_array(arr, "queries", |y, s| parser.parse_saved_query_str(y, s))?)
        }
        _ => None,
    };

    let layer = SemanticLayer::with_motifs_and_queries(views, topics, motifs, saved_queries);
    let entries = catalog::catalog(&layer);

    serde_wasm_bindgen::to_value(&entries).map_err(|e| JsValue::from_str(&e.to_string()))
}
