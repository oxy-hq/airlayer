//! ClickHouse query executor via HTTP interface.
//!
//! ClickHouse exposes a simple HTTP API: POST the SQL to `http://host:port/`
//! with optional auth headers. Results come back as tab-separated or JSON.

use super::{ClickHouseConnection, ExecutionResult};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &ClickHouseConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let host = config.get_host();
    let port = config.get_port();

    // Inline parameters (ClickHouse HTTP interface doesn't support bind params)
    let final_sql = inline_params(sql, params);

    // Request JSON output with column names and types
    let url = format!(
        "{}:{}/",
        host.trim_end_matches('/'),
        port,
    );

    let mut req = ureq::post(&url);

    if let Some(user) = config.get_user() {
        req = req.set("X-ClickHouse-User", &user);
    }
    if let Some(password) = config.get_password() {
        req = req.set("X-ClickHouse-Key", &password);
    }
    if let Some(ref db) = config.database {
        req = req.set("X-ClickHouse-Database", db);
    }

    // Append FORMAT JSONEachRow for structured output, unless the query already specifies a format
    let query_with_format = if final_sql.to_uppercase().contains(" FORMAT ") {
        final_sql.clone()
    } else {
        format!("{} FORMAT JSONCompactEachRow", final_sql)
    };

    // First, get column names via a separate query with LIMIT 0 + JSONCompact
    let columns = get_columns(&url, config, &final_sql)?;

    let resp = req
        .send_string(&query_with_format)
        .map_err(|e| EngineError::QueryError(format!("ClickHouse query failed: {}", e)))?;

    let body = resp
        .into_string()
        .map_err(|e| EngineError::QueryError(format!("Failed to read ClickHouse response: {}", e)))?;

    if body.trim().is_empty() {
        return Ok(ExecutionResult {
            columns,
            rows: vec![],
        });
    }

    let mut rows = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let arr: Vec<JsonValue> = serde_json::from_str(line).map_err(|e| {
            EngineError::QueryError(format!("Failed to parse ClickHouse row: {}", e))
        })?;

        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = arr.get(i).cloned().unwrap_or(JsonValue::Null);
            obj.insert(col_name.clone(), val);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

fn get_columns(
    url: &str,
    config: &ClickHouseConnection,
    sql: &str,
) -> Result<Vec<String>, EngineError> {
    let col_query = format!("{} LIMIT 0 FORMAT JSONCompact", sql);

    let mut req = ureq::post(url);
    if let Some(user) = config.get_user() {
        req = req.set("X-ClickHouse-User", &user);
    }
    if let Some(password) = config.get_password() {
        req = req.set("X-ClickHouse-Key", &password);
    }
    if let Some(ref db) = config.database {
        req = req.set("X-ClickHouse-Database", db);
    }

    let resp = req
        .send_string(&col_query)
        .map_err(|e| EngineError::QueryError(format!("ClickHouse column query failed: {}", e)))?;

    let json: JsonValue = resp
        .into_json()
        .map_err(|e| EngineError::QueryError(format!("Failed to parse column metadata: {}", e)))?;

    let meta = json["meta"].as_array().cloned().unwrap_or_default();
    Ok(meta
        .iter()
        .map(|m| m["name"].as_str().unwrap_or("unknown").to_string())
        .collect())
}

/// Inline $1, $2, ... parameters into the SQL as escaped string literals.
fn inline_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let escaped = param.replace('\'', "\\'");
        result = result.replace(&placeholder, &format!("'{}'", escaped));
    }
    result
}
