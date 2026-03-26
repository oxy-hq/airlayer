//! SQLite query executor via rusqlite.

use super::{ExecutionResult, SqliteConnection};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &SqliteConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let conn = match &config.path {
        Some(p) if !p.is_empty() => rusqlite::Connection::open(p),
        _ => rusqlite::Connection::open_in_memory(),
    }
    .map_err(|e| EngineError::QueryError(format!("Failed to open SQLite: {}", e)))?;

    // SQLite uses ? params natively
    let rewritten = rewrite_params(sql);

    let mut stmt = conn.prepare(&rewritten).map_err(|e| {
        EngineError::QueryError(format!("SQLite prepare failed: {}", e))
    })?;

    let columns: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();

    let mut query_rows = stmt
        .query(param_refs.as_slice())
        .map_err(|e| EngineError::QueryError(format!("SQLite query failed: {}", e)))?;

    let mut rows = Vec::new();
    while let Some(row) = query_rows
        .next()
        .map_err(|e| EngineError::QueryError(format!("SQLite row iteration failed: {}", e)))?
    {
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = sqlite_value_to_json(row, i);
            obj.insert(col_name.clone(), val);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

fn rewrite_params(sql: &str) -> String {
    let re = regex::Regex::new(r"\$(\d+)").unwrap();
    re.replace_all(sql, "?").to_string()
}

fn sqlite_value_to_json(row: &rusqlite::Row, idx: usize) -> JsonValue {
    use rusqlite::types::ValueRef;

    match row.get_ref(idx) {
        Ok(ValueRef::Null) => JsonValue::Null,
        Ok(ValueRef::Integer(n)) => JsonValue::Number(n.into()),
        Ok(ValueRef::Real(f)) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Ok(ValueRef::Text(s)) => {
            JsonValue::String(String::from_utf8_lossy(s).to_string())
        }
        Ok(ValueRef::Blob(b)) => {
            JsonValue::String(format!("<blob {} bytes>", b.len()))
        }
        Err(_) => JsonValue::Null,
    }
}
