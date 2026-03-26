//! Snowflake query executor via REST API (session-based login + query).
//!
//! Uses the same approach as the integration tests — authenticate via
//! `/session/v1/login-request`, then execute SQL via `/queries/v1/query-request`.
//! No heavy SDK dependency, just `ureq` + `serde_json`.

use super::{ExecutionResult, SnowflakeConnection};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(1);

struct SnowflakeSession {
    account: String,
    token: String,
}

fn authenticate(config: &SnowflakeConnection) -> Result<SnowflakeSession, EngineError> {
    let account = config.get_account()?;
    let username = config.get_username()?;
    let password = config.get_password()?;

    let url = format!(
        "https://{}.snowflakecomputing.com/session/v1/login-request",
        account,
    );

    let body = serde_json::json!({
        "data": {
            "LOGIN_NAME": username,
            "PASSWORD": password,
            "ACCOUNT_NAME": account,
        }
    });

    let resp = ureq::post(&url)
        .set("Content-Type", "application/json")
        .set("Accept", "application/json")
        .send_string(&body.to_string())
        .map_err(|e| EngineError::QueryError(format!("Snowflake auth failed: {}", e)))?;

    let json: JsonValue = resp
        .into_json()
        .map_err(|e| EngineError::QueryError(format!("Failed to parse auth response: {}", e)))?;

    if !json["success"].as_bool().unwrap_or(false) {
        return Err(EngineError::QueryError(format!(
            "Snowflake auth failed: {}",
            json["message"].as_str().unwrap_or("unknown error")
        )));
    }

    let token = json["data"]["token"]
        .as_str()
        .ok_or_else(|| EngineError::QueryError("No token in auth response".to_string()))?
        .to_string();

    Ok(SnowflakeSession { account, token })
}

fn execute_single(session: &SnowflakeSession, sql: &str) -> Result<JsonValue, EngineError> {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);

    let request_id = format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (seq.wrapping_mul(2654435761)) as u32,
        (seq.wrapping_mul(40503)) as u16,
        (seq.wrapping_mul(12345)) as u16 & 0xFFF,
        0x8000 | ((seq.wrapping_mul(54321)) as u16 & 0x3FFF),
        seq.wrapping_mul(1099511628211u64),
    );

    let url = format!(
        "https://{}.snowflakecomputing.com/queries/v1/query-request?requestId={}",
        session.account, request_id,
    );

    let body = serde_json::json!({
        "sqlText": sql,
        "asyncExec": false,
        "sequenceId": seq,
    });

    let result = ureq::post(&url)
        .set(
            "Authorization",
            &format!("Snowflake Token=\"{}\"", session.token),
        )
        .set("Content-Type", "application/json")
        .set("Accept", "application/snowflake")
        .send_string(&body.to_string());

    match result {
        Ok(resp) => resp
            .into_json::<JsonValue>()
            .map_err(|e| EngineError::QueryError(format!("Failed to parse response: {}", e))),
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            Err(EngineError::QueryError(format!(
                "Snowflake API error (HTTP {}): {}",
                code, body
            )))
        }
        Err(e) => Err(EngineError::QueryError(format!(
            "Snowflake API error: {}",
            e
        ))),
    }
}

pub fn execute(
    config: &SnowflakeConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let session = authenticate(config)?;

    // Set context via USE statements
    let mut context_stmts = Vec::new();
    if let Some(ref role) = config.role {
        context_stmts.push(format!("USE ROLE {}", role));
    }
    context_stmts.push(format!("USE WAREHOUSE {}", config.get_warehouse()));
    if let Some(db) = config.get_database() {
        context_stmts.push(format!("USE DATABASE {}", db));
    }
    if let Some(schema) = config.get_schema() {
        context_stmts.push(format!("USE SCHEMA {}", schema));
    }

    for stmt in &context_stmts {
        let resp = execute_single(&session, stmt)?;
        if !resp["success"].as_bool().unwrap_or(true) {
            return Err(EngineError::QueryError(format!(
                "Snowflake context setup failed: {}",
                resp["message"].as_str().unwrap_or("unknown")
            )));
        }
    }

    // Inline parameters (Snowflake REST API doesn't support bind params natively)
    let final_sql = inline_params(sql, params);

    let resp = execute_single(&session, &final_sql)?;

    if !resp["success"].as_bool().unwrap_or(true) {
        return Err(EngineError::QueryError(format!(
            "Snowflake query failed: {}",
            resp["message"].as_str().unwrap_or("unknown")
        )));
    }

    // Parse the response: data.rowtype has column metadata, data.rowset has rows
    let row_types = resp["data"]["rowtype"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let rowset = resp["data"]["rowset"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let columns: Vec<String> = row_types
        .iter()
        .map(|rt| {
            rt["name"]
                .as_str()
                .unwrap_or("unknown")
                .to_string()
        })
        .collect();

    let mut rows = Vec::with_capacity(rowset.len());
    for row_arr in &rowset {
        let cells = row_arr.as_array().cloned().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let val = cells.get(i).cloned().unwrap_or(JsonValue::Null);
            // Snowflake returns all values as strings in the rowset; try to parse numbers
            let typed_val = coerce_snowflake_value(&val, row_types.get(i));
            obj.insert(col_name.clone(), typed_val);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

/// Inline parameters into the SQL as escaped string literals.
/// Handles both `?` (Snowflake dialect) and `$1, $2, ...` (Postgres-style) placeholders.
fn inline_params(sql: &str, params: &[String]) -> String {
    if params.is_empty() {
        return sql.to_string();
    }

    // If SQL contains ?, use positional replacement
    if sql.contains('?') {
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
        return result;
    }

    // Otherwise, replace $1, $2, ... in reverse order
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let escaped = param.replace('\'', "''");
        result = result.replace(&placeholder, &format!("'{}'", escaped));
    }
    result
}

/// Try to coerce Snowflake's string values to appropriate JSON types based on rowtype metadata.
fn coerce_snowflake_value(val: &JsonValue, row_type: Option<&JsonValue>) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    let s = match val.as_str() {
        Some(s) => s,
        None => return val.clone(), // Already typed (shouldn't happen but be safe)
    };

    // Check Snowflake type from rowtype metadata
    if let Some(rt) = row_type {
        let sf_type = rt["type"].as_str().unwrap_or("");
        match sf_type {
            "fixed" => {
                // Integer or decimal based on scale
                let scale = rt["scale"].as_i64().unwrap_or(0);
                if scale == 0 {
                    if let Ok(n) = s.parse::<i64>() {
                        return JsonValue::Number(n.into());
                    }
                }
                if let Ok(n) = s.parse::<f64>() {
                    return serde_json::Number::from_f64(n)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::String(s.to_string()));
                }
            }
            "real" => {
                if let Ok(n) = s.parse::<f64>() {
                    return serde_json::Number::from_f64(n)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::String(s.to_string()));
                }
            }
            "boolean" => {
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
