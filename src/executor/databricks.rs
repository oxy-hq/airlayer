//! Databricks SQL executor via REST API (Statement Execution API).
//!
//! Uses Databricks SQL Statement Execution API:
//! POST /api/2.0/sql/statements with personal access token auth.

use super::{DatabricksConnection, ExecutionResult};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &DatabricksConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let host = config.get_host()?;
    let token = config.get_token()?;
    let warehouse_id = config.get_warehouse_id()?;

    let final_sql = inline_params(sql, params);

    let url = format!("https://{}/api/2.0/sql/statements", host);

    let mut body = serde_json::json!({
        "statement": final_sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
        "disposition": "INLINE",
    });

    if let Some(ref catalog) = config.catalog {
        body["catalog"] = JsonValue::String(catalog.clone());
    }
    if let Some(ref schema) = config.schema {
        body["schema"] = JsonValue::String(schema.clone());
    }

    let resp = ureq::post(&url)
        .set("Authorization", &format!("Bearer {}", token))
        .set("Content-Type", "application/json")
        .send_string(&body.to_string())
        .map_err(|e| EngineError::QueryError(format!("Databricks request failed: {}", e)))?;

    let json: JsonValue = resp.into_json().map_err(|e| {
        EngineError::QueryError(format!("Failed to parse Databricks response: {}", e))
    })?;

    let status = json["status"]["state"].as_str().unwrap_or("");
    match status {
        "SUCCEEDED" => {}
        "FAILED" => {
            let msg = json["status"]["error"]["message"]
                .as_str()
                .unwrap_or("unknown error");
            return Err(EngineError::QueryError(format!(
                "Databricks query failed: {}",
                msg
            )));
        }
        "PENDING" | "RUNNING" => {
            return Err(EngineError::QueryError(
                "Databricks query timed out (still running after 30s)".to_string(),
            ));
        }
        other => {
            return Err(EngineError::QueryError(format!(
                "Databricks unexpected status: {}",
                other
            )));
        }
    }

    let schema_cols = json["manifest"]["schema"]["columns"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let columns: Vec<String> = schema_cols
        .iter()
        .map(|c| c["name"].as_str().unwrap_or("unknown").to_string())
        .collect();

    let data_array = json["result"]["data_array"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let mut rows = Vec::with_capacity(data_array.len());
    for row_arr in &data_array {
        let cells = row_arr.as_array().cloned().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let raw = cells.get(i).cloned().unwrap_or(JsonValue::Null);
            let typed = coerce_databricks_value(&raw, schema_cols.get(i));
            obj.insert(col_name.clone(), typed);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

fn coerce_databricks_value(val: &JsonValue, col_meta: Option<&JsonValue>) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    let s = match val.as_str() {
        Some(s) => s,
        None => return val.clone(),
    };

    if let Some(meta) = col_meta {
        let type_name = meta["type_name"].as_str().unwrap_or("");
        match type_name {
            "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "LONG" => {
                if let Ok(n) = s.parse::<i64>() {
                    return JsonValue::Number(n.into());
                }
            }
            "FLOAT" | "DOUBLE" | "DECIMAL" => {
                if let Ok(n) = s.parse::<f64>() {
                    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
                        return JsonValue::Number((n as i64).into());
                    }
                    return serde_json::Number::from_f64(n)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::String(s.to_string()));
                }
            }
            "BOOLEAN" => {
                return match s {
                    "true" | "TRUE" | "1" => JsonValue::Bool(true),
                    "false" | "FALSE" | "0" => JsonValue::Bool(false),
                    _ => JsonValue::String(s.to_string()),
                };
            }
            _ => {}
        }
    }

    JsonValue::String(s.to_string())
}

/// Inline ? parameters into the SQL as escaped string literals.
fn inline_params(sql: &str, params: &[String]) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut param_idx = 0;
    for ch in sql.chars() {
        if ch == '?' && param_idx < params.len() {
            let escaped = params[param_idx].replace('\'', "''");
            result.push_str(&format!("'{}'", escaped));
            param_idx += 1;
        } else {
            result.push(ch);
        }
    }
    result
}
