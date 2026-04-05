//! Presto/Trino query executor via HTTP interface.
//!
//! Presto exposes a REST API at `http://host:port/v1/statement`.
//! We POST the SQL, then poll the `nextUri` until completion.

use super::{ExecutionResult, PrestoConnection};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &PrestoConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let host = config.get_host();
    let port = config.get_port();
    let user = config.get_user();

    // Inline parameters (Presto HTTP API doesn't support bind params)
    let final_sql = inline_params(sql, params);

    let url = format!("{}:{}/v1/statement", host.trim_end_matches('/'), port);

    let mut req = ureq::post(&url)
        .set("X-Presto-User", &user)
        .set("X-Trino-User", &user); // Trino compat

    if let Some(ref catalog) = config.catalog {
        req = req
            .set("X-Presto-Catalog", catalog)
            .set("X-Trino-Catalog", catalog);
    }
    if let Some(ref schema) = config.schema {
        req = req
            .set("X-Presto-Schema", schema)
            .set("X-Trino-Schema", schema);
    }
    if let Some(password) = config.get_password() {
        // Basic auth: base64(user:password)
        let credentials = base64_encode(&format!("{}:{}", user, password));
        req = req.set("Authorization", &format!("Basic {}", credentials));
    }

    let resp: JsonValue = req
        .send_string(&final_sql)
        .map_err(|e| EngineError::QueryError(format!("Presto query submission failed: {}", e)))?
        .into_json()
        .map_err(|e| EngineError::QueryError(format!("Failed to parse Presto response: {}", e)))?;

    // Poll nextUri until we get final results
    poll_until_complete(resp)
}

/// Maximum time to poll before giving up (5 minutes).
const POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Poll the Presto nextUri endpoint until the query completes.
/// Accumulates data from intermediate responses (Presto can return partial results).
fn poll_until_complete(mut resp: JsonValue) -> Result<ExecutionResult, EngineError> {
    let start = std::time::Instant::now();
    let mut columns: Vec<String> = Vec::new();
    let mut col_types: Vec<String> = Vec::new();
    let mut rows: Vec<serde_json::Map<String, JsonValue>> = Vec::new();

    loop {
        if start.elapsed() > POLL_TIMEOUT {
            return Err(EngineError::QueryError(
                "Presto query timed out after 5 minutes of polling".to_string(),
            ));
        }

        // Check for error
        if let Some(error) = resp.get("error") {
            let msg = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Presto error");
            return Err(EngineError::QueryError(format!("Presto error: {}", msg)));
        }

        // Extract column metadata from first response that has it
        if columns.is_empty() {
            if let Some(cols) = resp.get("columns").and_then(|c| c.as_array()) {
                columns = cols
                    .iter()
                    .map(|c| {
                        c.get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("unknown")
                            .to_string()
                    })
                    .collect();
                col_types = cols
                    .iter()
                    .map(|c| {
                        c.get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("")
                            .to_string()
                    })
                    .collect();
            }
        }

        // Accumulate data from this response (Presto can return partial results)
        if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
            for row_arr in data {
                let cells = row_arr.as_array().cloned().unwrap_or_default();
                let mut obj = serde_json::Map::new();
                for (i, col_name) in columns.iter().enumerate() {
                    let val = cells.get(i).cloned().unwrap_or(JsonValue::Null);
                    let typed = coerce_presto_value(
                        &val,
                        col_types.get(i).map(|s| s.as_str()).unwrap_or(""),
                    );
                    obj.insert(col_name.clone(), typed);
                }
                rows.push(obj);
            }
        }

        // If there's a nextUri, keep polling
        if let Some(next_uri) = resp.get("nextUri").and_then(|u| u.as_str()) {
            std::thread::sleep(std::time::Duration::from_millis(100));
            resp = ureq::get(next_uri)
                .call()
                .map_err(|e| EngineError::QueryError(format!("Presto poll failed: {}", e)))?
                .into_json()
                .map_err(|e| {
                    EngineError::QueryError(format!("Failed to parse Presto poll response: {}", e))
                })?;
            continue;
        }

        // No nextUri means we're done
        return Ok(ExecutionResult { columns, rows });
    }
}

/// Coerce Presto values based on column type metadata.
fn coerce_presto_value(val: &JsonValue, presto_type: &str) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    let lower = presto_type.to_lowercase();

    if let Some(s) = val.as_str() {
        if lower.contains("int") {
            if let Ok(n) = s.parse::<i64>() {
                return JsonValue::Number(n.into());
            }
        }
        if lower.contains("double") || lower.contains("real") || lower.contains("decimal") {
            if let Ok(f) = s.parse::<f64>() {
                return serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or(val.clone());
            }
        }
    }

    val.clone()
}

/// Inline ? parameters into the SQL as escaped string literals.
fn inline_params(sql: &str, params: &[String]) -> String {
    if params.is_empty() {
        return sql.to_string();
    }
    let mut result = String::with_capacity(sql.len());
    let mut param_idx = 0;
    for ch in sql.chars() {
        if ch == '?' && param_idx < params.len() {
            let escaped = params[param_idx].replace('\'', "''");
            result.push('\'');
            result.push_str(&escaped);
            result.push('\'');
            param_idx += 1;
        } else {
            result.push(ch);
        }
    }
    result
}

/// Simple base64 encoding for Basic auth (no external dep needed).
fn base64_encode(input: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_params_basic() {
        let sql = "SELECT * FROM t WHERE x = ? AND y = ?";
        let result = inline_params(sql, &["hello".into(), "world".into()]);
        assert_eq!(result, "SELECT * FROM t WHERE x = 'hello' AND y = 'world'");
    }

    #[test]
    fn test_inline_params_single_quote_escaped() {
        let sql = "SELECT * FROM t WHERE x = ?";
        let result = inline_params(sql, &["it's a test".into()]);
        assert_eq!(result, "SELECT * FROM t WHERE x = 'it''s a test'");
    }

    #[test]
    fn test_inline_params_empty() {
        let sql = "SELECT 1";
        let result = inline_params(sql, &[]);
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode("user:pass"), "dXNlcjpwYXNz");
    }

    #[test]
    fn test_coerce_presto_integer() {
        let val = JsonValue::String("42".into());
        let result = coerce_presto_value(&val, "integer");
        assert_eq!(result, JsonValue::Number(42.into()));
    }

    #[test]
    fn test_coerce_presto_null() {
        let result = coerce_presto_value(&JsonValue::Null, "varchar");
        assert_eq!(result, JsonValue::Null);
    }
}
