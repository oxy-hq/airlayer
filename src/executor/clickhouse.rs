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

    // Use JSONCompact format — returns {"meta":[...], "data":[[...], ...]} with typed values
    let query_with_format = if final_sql.to_uppercase().contains(" FORMAT ") {
        final_sql.clone()
    } else {
        format!("{} FORMAT JSONCompact", final_sql)
    };

    let resp = req
        .send_string(&query_with_format)
        .map_err(|e| EngineError::QueryError(format!("ClickHouse query failed: {}", e)))?;

    let json: JsonValue = resp
        .into_json()
        .map_err(|e| EngineError::QueryError(format!("Failed to parse ClickHouse response: {}", e)))?;

    let meta = json["meta"].as_array().cloned().unwrap_or_default();
    let data = json["data"].as_array().cloned().unwrap_or_default();

    let columns: Vec<String> = meta
        .iter()
        .map(|m| m["name"].as_str().unwrap_or("unknown").to_string())
        .collect();

    let col_types: Vec<String> = meta
        .iter()
        .map(|m| m["type"].as_str().unwrap_or("").to_string())
        .collect();

    let mut rows = Vec::with_capacity(data.len());
    for row_arr in &data {
        let cells = row_arr.as_array().cloned().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = cells.get(i).cloned().unwrap_or(JsonValue::Null);
            let typed = coerce_clickhouse_value(&val, col_types.get(i).map(|s| s.as_str()).unwrap_or(""));
            obj.insert(col_name.clone(), typed);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

/// Coerce ClickHouse values based on column type metadata.
fn coerce_clickhouse_value(val: &JsonValue, ch_type: &str) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    // ClickHouse JSONCompact returns numbers as strings for large integer types
    if let Some(s) = val.as_str() {
        if ch_type.contains("Int") || ch_type == "UInt64" || ch_type == "UInt32" {
            if let Ok(n) = s.parse::<i64>() {
                return JsonValue::Number(n.into());
            }
        }
        if ch_type.contains("Float") || ch_type.contains("Decimal") {
            if let Ok(f) = s.parse::<f64>() {
                return serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or(val.clone());
            }
        }
    }

    val.clone()
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
