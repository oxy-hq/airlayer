//! Domo query executor via REST API.
//!
//! Domo uses a developer token + dataset ID to execute SQL queries.
//! POST /api/query/v1/execute/{dataset_id}

use super::{DomoConnection, ExecutionResult};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &DomoConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let token = config.get_developer_token()?;
    let dataset_id = &config.dataset_id;
    let instance = &config.instance;

    let final_sql = inline_params(sql, params);

    let url = format!(
        "https://{}.domo.com/api/query/v1/execute/{}",
        instance, dataset_id
    );

    let body = serde_json::json!({
        "sql": final_sql,
    });

    let resp = ureq::post(&url)
        .set("X-DOMO-Developer-Token", &token)
        .set("Content-Type", "application/json")
        .send_string(&body.to_string())
        .map_err(|e| EngineError::QueryError(format!("Domo request failed: {}", e)))?;

    let json: JsonValue = resp
        .into_json()
        .map_err(|e| EngineError::QueryError(format!("Failed to parse Domo response: {}", e)))?;

    let col_names = json["columns"].as_array().cloned().unwrap_or_default();
    let metadata = json["metadata"].as_array().cloned().unwrap_or_default();
    let data_rows = json["rows"].as_array().cloned().unwrap_or_default();

    let columns: Vec<String> = col_names
        .iter()
        .map(|c| c.as_str().unwrap_or("unknown").to_string())
        .collect();

    let mut rows = Vec::with_capacity(data_rows.len());
    for row_arr in &data_rows {
        let cells = row_arr.as_array().cloned().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let raw = cells.get(i).cloned().unwrap_or(JsonValue::Null);
            let typed = coerce_domo_value(&raw, metadata.get(i));
            obj.insert(col_name.clone(), typed);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

fn coerce_domo_value(val: &JsonValue, meta: Option<&JsonValue>) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    // Domo may return values already typed, or as strings
    if let Some(meta) = meta {
        let domo_type = meta["type"].as_str().unwrap_or("");
        match domo_type {
            "LONG" => {
                if let Some(n) = val.as_i64() {
                    return JsonValue::Number(n.into());
                }
                if let Some(s) = val.as_str() {
                    if let Ok(n) = s.parse::<i64>() {
                        return JsonValue::Number(n.into());
                    }
                }
            }
            "DOUBLE" | "DECIMAL" => {
                if let Some(f) = val.as_f64() {
                    return serde_json::Number::from_f64(f)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null);
                }
                if let Some(s) = val.as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return serde_json::Number::from_f64(f)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null);
                    }
                }
            }
            "BOOLEAN" => {
                if let Some(b) = val.as_bool() {
                    return JsonValue::Bool(b);
                }
            }
            _ => {}
        }
    }

    val.clone()
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
