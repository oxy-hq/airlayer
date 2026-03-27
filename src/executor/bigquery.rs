//! BigQuery query executor via REST API.
//!
//! Supports two auth methods:
//! - `access_token` / `access_token_var`: Pre-obtained OAuth2 token (e.g., from `gcloud auth print-access-token`)
//! - `key_path`: Path to a service account JSON key file (requires `jsonwebtoken` — not yet implemented, use access_token for now)

use super::{BigQueryConnection, ExecutionResult};
use crate::engine::EngineError;
use serde_json::Value as JsonValue;

pub fn execute(
    config: &BigQueryConnection,
    sql: &str,
    params: &[String],
) -> Result<ExecutionResult, EngineError> {
    let project = config.get_project()?;
    let token = config.get_access_token()?;

    let final_sql = inline_params(sql, params);

    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
        project
    );

    let mut body = serde_json::json!({
        "query": final_sql,
        "useLegacySql": false,
        "maxResults": 10000,
    });

    if let Some(ref dataset) = config.dataset {
        body["defaultDataset"] = serde_json::json!({
            "projectId": project,
            "datasetId": dataset,
        });
    }

    let resp = ureq::post(&url)
        .set("Authorization", &format!("Bearer {}", token))
        .set("Content-Type", "application/json")
        .send_string(&body.to_string())
        .map_err(|e| EngineError::QueryError(format!("BigQuery request failed: {}", e)))?;

    let json: JsonValue = resp
        .into_json()
        .map_err(|e| EngineError::QueryError(format!("Failed to parse BigQuery response: {}", e)))?;

    if let Some(errors) = json["errors"].as_array() {
        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| e["message"].as_str().unwrap_or("unknown"))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(EngineError::QueryError(format!(
                "BigQuery query failed: {}",
                msg
            )));
        }
    }

    // Check for error in status
    if let Some(err) = json.get("error") {
        return Err(EngineError::QueryError(format!(
            "BigQuery error: {}",
            err["message"].as_str().unwrap_or("unknown")
        )));
    }

    let schema_fields = json["schema"]["fields"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let columns: Vec<String> = schema_fields
        .iter()
        .map(|f| f["name"].as_str().unwrap_or("unknown").to_string())
        .collect();

    let bq_rows = json["rows"].as_array().cloned().unwrap_or_default();

    let mut rows = Vec::with_capacity(bq_rows.len());
    for bq_row in &bq_rows {
        let cells = bq_row["f"].as_array().cloned().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let raw = cells
                .get(i)
                .and_then(|c| c.get("v"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            let typed = coerce_bigquery_value(&raw, schema_fields.get(i));
            obj.insert(col_name.clone(), typed);
        }
        rows.push(obj);
    }

    Ok(ExecutionResult { columns, rows })
}

fn coerce_bigquery_value(val: &JsonValue, field: Option<&JsonValue>) -> JsonValue {
    if val.is_null() {
        return JsonValue::Null;
    }

    let s = match val.as_str() {
        Some(s) => s,
        None => return val.clone(),
    };

    if let Some(field) = field {
        let bq_type = field["type"].as_str().unwrap_or("");
        match bq_type {
            "INTEGER" | "INT64" => {
                if let Ok(n) = s.parse::<i64>() {
                    return JsonValue::Number(n.into());
                }
            }
            "FLOAT" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC" => {
                if let Ok(n) = s.parse::<f64>() {
                    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
                        return JsonValue::Number((n as i64).into());
                    }
                    return serde_json::Number::from_f64(n)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::String(s.to_string()));
                }
            }
            "BOOLEAN" | "BOOL" => {
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

/// Inline @p0, @p1, ... parameters into the SQL as escaped string literals.
fn inline_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("@p{}", i);
        let escaped = param.replace('\'', "''");
        result = result.replace(&placeholder, &format!("'{}'", escaped));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_params_basic() {
        let sql = "SELECT * FROM t WHERE x = @p0 AND y = @p1";
        let result = inline_params(sql, &["hello".into(), "world".into()]);
        assert_eq!(result, "SELECT * FROM t WHERE x = 'hello' AND y = 'world'");
    }

    #[test]
    fn test_inline_params_single_quote_escaped() {
        let sql = "SELECT * FROM t WHERE x = @p0";
        let result = inline_params(sql, &["it's a test".into()]);
        assert_eq!(result, "SELECT * FROM t WHERE x = 'it''s a test'");
    }

    #[test]
    fn test_inline_params_empty() {
        let sql = "SELECT 1";
        let result = inline_params(sql, &[]);
        assert_eq!(result, "SELECT 1");
    }
}
