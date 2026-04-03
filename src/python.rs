//! Python bindings for airlayer.
//!
//! Provides a Python-friendly API for compiling semantic queries to SQL.
//! Build with: `maturin develop --no-default-features --features python`

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::dialect::Dialect;
use crate::engine::catalog;
use crate::engine::query::QueryRequest;
use crate::engine::{DatasourceDialectMap, SemanticEngine};
use crate::schema::models::SemanticLayer;
use crate::schema::parser::SchemaParser;

/// Compile a semantic query to SQL.
///
/// Args:
///     views_yaml: List of .view.yml file contents (YAML strings).
///     query_json: Query as JSON string (same format as ``airlayer query -q``).
///     dialect: SQL dialect string (e.g., "postgres", "bigquery", "duckdb").
///     topics_yaml: Optional list of .topic.yml file contents.
///     motifs_yaml: Optional list of .motif.yml file contents.
///     queries_yaml: Optional list of .query.yml file contents (saved queries).
///
/// Returns:
///     dict with ``sql``, ``params``, and ``columns`` keys.
///
/// Raises:
///     ValueError: If inputs are invalid or compilation fails.
#[pyfunction]
#[pyo3(signature = (views_yaml, query_json, dialect, topics_yaml=None, motifs_yaml=None, queries_yaml=None))]
fn compile(
    views_yaml: Vec<String>,
    query_json: &str,
    dialect: &str,
    topics_yaml: Option<Vec<String>>,
    motifs_yaml: Option<Vec<String>>,
    queries_yaml: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let parser = SchemaParser::new();

    let views: Vec<_> = views_yaml
        .iter()
        .enumerate()
        .map(|(i, y)| parser.parse_view_str(y, &format!("<view_{i}>")))
        .collect::<Result<_, _>>()
        .map_err(|e| PyValueError::new_err(e))?;

    let topics = match topics_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let t: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_topic_str(y, &format!("<topic_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(t)
        }
        _ => None,
    };

    let motifs = match motifs_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let m: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_motif_str(y, &format!("<motif_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(m)
        }
        _ => None,
    };

    let saved_queries = match queries_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let q: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_saved_query_str(y, &format!("<query_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(q)
        }
        _ => None,
    };

    let layer = SemanticLayer::with_motifs_and_queries(views, topics, motifs, saved_queries);

    let resolved_dialect = Dialect::from_str(dialect)
        .ok_or_else(|| PyValueError::new_err(format!("Unknown dialect: {dialect}")))?;

    let mut dialect_map = DatasourceDialectMap::new();
    dialect_map.set_default(resolved_dialect);

    let engine = SemanticEngine::from_semantic_layer(layer, dialect_map)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    let request: QueryRequest = serde_json::from_str(query_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid query JSON: {e}")))?;

    let result = engine
        .compile_query(&request)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    // Convert to Python dict
    let json_value =
        serde_json::to_value(&result).map_err(|e| PyValueError::new_err(e.to_string()))?;

    Python::with_gil(|py| json_to_py(py, &json_value))
}

/// Validate view YAML without compiling a query.
///
/// Args:
///     views_yaml: List of .view.yml file contents (YAML strings).
///     topics_yaml: Optional list of .topic.yml file contents.
///
/// Returns:
///     True if all views parse and validate successfully.
///
/// Raises:
///     ValueError: If validation fails.
#[pyfunction]
#[pyo3(signature = (views_yaml, topics_yaml=None))]
fn validate(views_yaml: Vec<String>, topics_yaml: Option<Vec<String>>) -> PyResult<bool> {
    let parser = SchemaParser::new();

    let views: Vec<_> = views_yaml
        .iter()
        .enumerate()
        .map(|(i, y)| parser.parse_view_str(y, &format!("<view_{i}>")))
        .collect::<Result<_, _>>()
        .map_err(|e| PyValueError::new_err(e))?;

    let topics = match topics_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let t: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_topic_str(y, &format!("<topic_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(t)
        }
        _ => None,
    };

    let layer = SemanticLayer::new(views, topics);

    crate::schema::validator::SchemaValidator::validate(&layer)
        .map_err(|e| PyValueError::new_err(e))?;

    Ok(true)
}

/// Convert a serde_json::Value to a Python object.
fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let items: Vec<PyObject> = arr
                .iter()
                .map(|v| json_to_py(py, v))
                .collect::<PyResult<_>>()?;
            Ok(items.into_pyobject(py)?.into_any().unbind())
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// List all semantic objects (views, dimensions, measures, motifs, etc.).
///
/// Args:
///     views_yaml: List of .view.yml file contents (YAML strings).
///     topics_yaml: Optional list of .topic.yml file contents.
///     motifs_yaml: Optional list of .motif.yml file contents.
///     queries_yaml: Optional list of .query.yml file contents (saved queries).
///
/// Returns:
///     list of dicts, each with ``kind``, ``name``, ``description``, ``view``,
///     ``member_type``, and ``meta`` keys.
///
/// Raises:
///     ValueError: If inputs are invalid.
#[pyfunction]
#[pyo3(signature = (views_yaml, topics_yaml=None, motifs_yaml=None, queries_yaml=None))]
fn catalog_list(
    views_yaml: Vec<String>,
    topics_yaml: Option<Vec<String>>,
    motifs_yaml: Option<Vec<String>>,
    queries_yaml: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let parser = SchemaParser::new();

    let views: Vec<_> = views_yaml
        .iter()
        .enumerate()
        .map(|(i, y)| parser.parse_view_str(y, &format!("<view_{i}>")))
        .collect::<Result<_, _>>()
        .map_err(|e| PyValueError::new_err(e))?;

    let topics = match topics_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let t: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_topic_str(y, &format!("<topic_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(t)
        }
        _ => None,
    };

    let motifs = match motifs_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let m: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_motif_str(y, &format!("<motif_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(m)
        }
        _ => None,
    };

    let saved_queries = match queries_yaml {
        Some(ref arr) if !arr.is_empty() => {
            let q: Vec<_> = arr
                .iter()
                .enumerate()
                .map(|(i, y)| parser.parse_saved_query_str(y, &format!("<query_{i}>")))
                .collect::<Result<_, _>>()
                .map_err(|e| PyValueError::new_err(e))?;
            Some(q)
        }
        _ => None,
    };

    let layer = SemanticLayer::with_motifs_and_queries(views, topics, motifs, saved_queries);
    let entries = catalog::catalog(&layer);

    let json_value =
        serde_json::to_value(&entries).map_err(|e| PyValueError::new_err(e.to_string()))?;

    Python::with_gil(|py| json_to_py(py, &json_value))
}

/// The airlayer Python module.
#[pymodule]
fn airlayer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compile, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(catalog_list, m)?)?;
    Ok(())
}
