//! WebAssembly bindings for airlayer.
//!
//! Provides a JS-friendly API for compiling semantic queries to SQL.
//! Build with: `wasm-pack build --target web --no-default-features --features wasm`

use wasm_bindgen::prelude::*;

use crate::dialect::Dialect;
use crate::engine::query::QueryRequest;
use crate::engine::{DatasourceDialectMap, SemanticEngine};
use crate::schema::models::SemanticLayer;
use crate::schema::parser::SchemaParser;

/// Compile a semantic query to SQL.
///
/// # Arguments
/// - `views_yaml`: Array of .view.yml file contents (YAML strings)
/// - `query_json`: Query as JSON (same format as `airlayer query -q`)
/// - `dialect`: SQL dialect string (e.g., "postgres", "bigquery", "duckdb")
///
/// # Returns
/// JSON object with `sql`, `params`, and `columns` fields.
#[wasm_bindgen]
pub fn compile(
    views_yaml: Vec<JsValue>,
    query_json: &str,
    dialect: &str,
) -> Result<JsValue, JsValue> {
    let parser = SchemaParser::new();

    let mut views = Vec::new();
    for (i, yaml_val) in views_yaml.iter().enumerate() {
        let yaml_str = yaml_val
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("views_yaml[{}] is not a string", i)))?;
        let view = parser
            .parse_view_str(&yaml_str, &format!("<view_{}>", i))
            .map_err(|e| JsValue::from_str(&e))?;
        views.push(view);
    }

    let layer = SemanticLayer::new(views, None);

    let resolved_dialect = Dialect::from_str(dialect)
        .ok_or_else(|| JsValue::from_str(&format!("Unknown dialect: {}", dialect)))?;

    let mut dialect_map = DatasourceDialectMap::new();
    dialect_map.set_default(resolved_dialect);

    let engine = SemanticEngine::from_semantic_layer(layer, dialect_map)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let request: QueryRequest =
        serde_json::from_str(query_json).map_err(|e| JsValue::from_str(&format!("Invalid query JSON: {}", e)))?;

    let result = engine
        .compile_query(&request)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    serde_wasm_bindgen::to_value(&result).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Validate view YAML without compiling a query.
///
/// # Arguments
/// - `views_yaml`: Array of .view.yml file contents (YAML strings)
///
/// # Returns
/// `true` if all views parse and validate successfully.
/// Throws on error with a descriptive message.
#[wasm_bindgen]
pub fn validate(views_yaml: Vec<JsValue>) -> Result<bool, JsValue> {
    let parser = SchemaParser::new();

    let mut views = Vec::new();
    for (i, yaml_val) in views_yaml.iter().enumerate() {
        let yaml_str = yaml_val
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("views_yaml[{}] is not a string", i)))?;
        let view = parser
            .parse_view_str(&yaml_str, &format!("<view_{}>", i))
            .map_err(|e| JsValue::from_str(&e))?;
        views.push(view);
    }

    let layer = SemanticLayer::new(views, None);

    crate::schema::validator::SchemaValidator::validate(&layer)
        .map_err(|e| JsValue::from_str(&e))?;

    Ok(true)
}
